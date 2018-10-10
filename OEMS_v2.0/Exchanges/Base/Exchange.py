import sys

sys.path.insert(0, '~/dev/crypto_oms')

import time
import ssl
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool
from pathos.multiprocessing import ProcessingPool as Pool
from utils.print_util import get_info_logger
from utils.funcs import run_function_safe
from utils.RouteFinder import RouteFinder
from .OrderBook import OrderBook
from .Fees import Fees
from .Orders import OrderInfo, OrderBatch
from .oms_coin_api_wrapper import OMSCoinAPIWrapper
import ccxt
import copy
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os
import pandas as pd
import json
import datetime

class Exchange:
    def __init__(self, exchange_name, api,
                 db_environ_url="DATABASE_URL_AWS_DEV",
                 coinapi_environ_url="ALTCOIN_COINAPI_KEY",
                 coinapi_environ_production_urls="ALTCOIN_COINAPI_PRODUCTION_URLS"):

        self.api = api
        self.log = get_info_logger('logs/' + exchange_name + str(time.time()) + ".log")
        self.coinapi = OMSCoinAPIWrapper(os.environ.get(coinapi_environ_url),
                                         exchange_name,
                                         os.environ.get(coinapi_environ_production_urls),
                                         self.log)
        self.exchange_name = exchange_name
        self.fees = defaultdict(Fees)
        #self.update_fees()
        self.balances = self.get_balances()
        self.order_books = defaultdict(OrderBook)
        tickers = self.get_tickers_safe()
        self.allMarketSyms = tickers.keys()
        self.allCoins = set()
        for mktSym in self.allMarketSyms:
            b,q=mktSym.split('/')
            self.allCoins.add(b)
            self.allCoins.add(q)
        self.routeFinder = RouteFinder(self.exchange_name.lower(), self.allMarketSyms)

        self.DB_ENGINE = create_engine(os.environ.get(db_environ_url))

        self.relevantSingleUnitBalances=None
        self.relevantMultipleUnitBalances=None

        self.validBalanceTypes = ['long_short_balance', 'borrow_balance', 'cushion_balance', 'reserve_currency_added']
        self.balanceKeys = {'long_short_balance':'lsBalance', 'borrow_balance':'borrowBalance', 'cushion_balance':'cushionBalance',
                            'reserve_currency_added':'reserveCurrencyAdded'}

    def log_it(self, level, msg, printMsg=True, *args, **kwargs):
        if (printMsg):
            print(datetime.datetime.now().strftime('%Y/%m/%d %I:%M:%S %p'), self.exchange_name, msg)
        if self.log is None:
            print(level, msg, args, kwargs)
        if level.lower() == 'info':
            self.log.info(msg, *args, **kwargs)
        elif level.lower() == 'error':
            self.log.error(msg, *args, **kwargs)
        elif level.lower() == "debug":
            self.log.debug(msg, *args, **kwargs)
        else:
            self.log.log(level, msg, *args, **kwargs)

    def update_balance(self):
        self.balances = self.get_balances()

    def update_ob_data(self, market):
        try:
            bids, asks = self.get_ob(market)
            bids = self.format_ob(bids)
            asks = self.format_ob(asks)
            self.order_books[market] = OrderBook(bids, asks)
            self.order_books[market].add_market_price(self.fees[market])
        except (ValueError, ssl.SSLError) as e:
            if self.log is not None:
                self.log.error(e)
            else:
                self.log_it('error', e)

    def update_fees(self):
        raise NotImplementedError("You need to implement this")

    def get_list_of_markets(self):
        markets = self.api.fetch_markets()
        markets_list = []
        for market in markets:
            if ('active' in market and market['active']) or ('active' not in market):
                markets_list.append(market['symbol'])
        return markets_list

    def get_ticker(self, market):
        if isinstance(market, tuple) or isinstance(market, list):
            assert len(market) == 2, "Need to only pass a list of len 2"
            market, dictionary = market
            dictionary[market] = self.api.fetch_ticker(market)
        else:
            return self.api.fetch_ticker(market)

    def get_ticker_safe(self, market):
        return run_function_safe(self.get_ticker, market)

    def get_tickers(self, markets, multiprocess=False):
        if multiprocess:
            if(markets is None):
                markets = self.get_list_of_markets()
            ticker_dictionary = {}
            args = []
            for market in markets:
                if ('active' in market and market['active']) or ('active' not in market):
                    args.append(([market, ticker_dictionary],))
            pool = ThreadPool(8)
            pool.starmap(self.get_ticker_safe, args)
            pool.close()
            pool.join()
            return ticker_dictionary
        else:
            return self.api.fetch_tickers(symbols=markets)

    def get_tickers_safe(self, symbols=None, multiprocess=False):
        if(self.useCoinapi):
            return self.coinapi.fetch_tickers(symbols)
        else:
            return run_function_safe(self.get_tickers, symbols, multiprocess)

    def check_health(self):
        attempts = 4
        for attempt in range(attempts):
            try:
                self.api.fetch_balance()
                return True
            except (ccxt.RequestTimeout, ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
                time.sleep(.2)
        return False

    def market_order(self, side, marketSym, amount):
        f = {
            'buy': self.market_buy,
            'sell': self.market_sell
        }[side]
        return f(marketSym, amount)

    def limit_order(self, side, marketSym, amount, rate):
        f = {
            'buy': self.limit_buy,
            'sell': self.limit_sell
        }[side]
        return f(marketSym, amount, rate)

    def smart_order(self, startCurrency, endCurrency, startCurrencyAmount):
        self.log_it('info', '==========START SMART ORDER==========')
        allRouts = self.routeFinder.find_routes(startCurrency, endCurrency)
        chosenRoute = self.choose_route(allRouts)
        self.log_it('info', self.routeFinder.route_to_string(chosenRoute))
        routeBatches = []
        for action in chosenRoute:
            routeBatches.append([action])
        routeResults = self.execute_routes(routeBatches, {startCurrency:startCurrencyAmount}, raiseMarketOfflineError=True)
        self.log_it('info', '==========END SMART ORDER==========')
        return routeResults

    def execute_routes(self, routeBatches, routeStartQuantities, raiseMarketOfflineError, worstPrice = None, resultDict = None):
        if(len(routeBatches)>1 and worstPrice is not None):
            raise NameError("""Cannot provide worst price when route has multiple hops. This could cause coins to get
            stranded in the middle of a route.""")
        curRouteBatchQuantities = copy.deepcopy(routeStartQuantities)

        routeBatchResults = {'orderInfo': [], 'errors': {'sellingTooMuch': [],
                                                    'buyingTooMuch': [],
                                                    'dustOrder': [],
                                                    'marketOffline': []}}
        routeQuantitiesHistory = []
        routeQuantitiesHistory.append(copy.deepcopy(routeStartQuantities))

        for routeBatch in routeBatches:
            #make/execute order batch with first orders in route batch
            orders = []
            for marketNameOrderType in routeBatch:
                _, base, quote, side = self.routeFinder.split_market_action(marketNameOrderType)

                if (side == 'BUY'):
                    curQuantity = curRouteBatchQuantities[quote] / self.get_ticker_safe('{}/{}'.format(base, quote))['ask']

                    orders.append(OrderInfo(side.lower(), '{}/{}'.format(base, quote), curQuantity, None, self,
                                            worstPrice=worstPrice, raiseMarketOfflineError=raiseMarketOfflineError))


                else:
                    orders.append(OrderInfo(side.lower(), '{}/{}'.format(base, quote), curRouteBatchQuantities[base], None, self,
                                            worstPrice=worstPrice, raiseMarketOfflineError=raiseMarketOfflineError))

            orderBatch = OrderBatch(orders, self)

            startBalances = self.get_balances()
            results = orderBatch.execute_batch()
            endBalances = self.get_balances()

            curRouteBatchQuantities = {}
            for marketNameOrderType in routeBatch:
                _, base, quote, side = self.routeFinder.split_market_action(marketNameOrderType)
                if(side == 'BUY'):
                    curRouteBatchQuantities[base] = endBalances[base]['free']-startBalances[base]['free']
                else:
                    curRouteBatchQuantities[quote] = endBalances[quote]['free']-startBalances[quote]['free']
            print(curRouteBatchQuantities)
            
            routeQuantitiesHistory.append(curRouteBatchQuantities)
            routeBatchResults['orderInfo'] += results['orderInfo']
            routeBatchResults['errors']['sellingTooMuch'] += results['errors']['sellingTooMuch']
            routeBatchResults['errors']['buyingTooMuch'] += results['errors']['buyingTooMuch']
            routeBatchResults['errors']['dustOrder'] += results['errors']['dustOrder']
            routeBatchResults['errors']['marketOffline'] += results['errors']['marketOffline']

        routeBatchResults['routeQuantitiesHistory'] = routeQuantitiesHistory

        if(resultDict is None):
            return routeBatchResults
        else:
            resultDict['result'] = routeBatchResults

    def liquidate(self,
                 coinToLiquidateTo,
                 coinsToLiquidate,
                 minBalanceUnit,
                 minBalance,
                 raiseMarketOfflineError):
        
        self.log_it('info', '==========START LIQUIDATE==========')

        #get multiple unit balances of coins to liquidate
        tickers = self.get_tickers_safe()
        tradableCoins = set()
        for mktSym in tickers:
            b,q=mktSym.split('/')
            tradableCoins.add(b)
            tradableCoins.add(q)
        balances=self.get_balances()
        multipleUnitBalance = {}
        for coin in balances['free']:
            if(((coinsToLiquidate is not None and coin in coinsToLiquidate) or (coinsToLiquidate is None))
                    and coin in tradableCoins):
                multipleUnitBalance[coin] = balances['free'][coin]

        #get balances over "minBalance"
        singleUnitBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(multipleUnitBalance,
                                                                                                      minBalanceUnit,
                                                                                                      tickers)
        for coin in singleUnitBalance:
            if(singleUnitBalance[coin] < minBalance or coin == coinToLiquidateTo):
                del multipleUnitBalance[coin]
                
        #get routeBatches
        routeBatches = []
        for coin in multipleUnitBalance:
            allRoutes = self.routeFinder.find_routes(coin, coinToLiquidateTo)
            chosenRoute = self.choose_route(allRoutes)
            if(len(routeBatches) < len(chosenRoute)):
                for i in range(len(chosenRoute)-len(routeBatches)):
                    routeBatches.append([])
            for i in range(len(chosenRoute)):
                routeBatches[i].append(chosenRoute[i])
        
        self.log_it('info', 'Route Batches: {}, Balances To Liquidate: {}'.format(routeBatches, multipleUnitBalance))
        results = self.execute_routes(routeBatches, multipleUnitBalance, raiseMarketOfflineError=raiseMarketOfflineError)

        self.log_it('info', '==========END LIQUIDATE==========')
        
        return results

    def update_relevant_balances(self, commonCurrency, currencyUnit, endSingleUnitPortfolio, tickers, usingBNBForFees):
        allBalances = self.get_balances()
        relevantCoins = set(endSingleUnitPortfolio.keys())
        relevantCoins.add(commonCurrency)
        relevantCoins.add(currencyUnit)
        if(usingBNBForFees):
            relevantCoins.add('BNB')
        relevantMultiUnitBalances = {}
        for coin in relevantCoins:
            relevantMultiUnitBalances[coin] = allBalances[coin]['free']

        relevantSingeUnitBalances = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            relevantMultiUnitBalances, currencyUnit, tickers)

        self.relevantMultipleUnitBalances=relevantMultiUnitBalances
        self.relevantSingleUnitBalances=relevantSingeUnitBalances

    def get_last_balance(self, botName, exchange, balanceType):
        assert balanceType in self.validBalanceTypes
        sql = 'select * from "{}".{}_{} order by in_z desc limit 1'.format(botName, exchange, balanceType)
        balanceInfo = pd.read_sql(sql, self.DB_ENGINE)
        balanceInfo = balanceInfo.loc[0].to_dict()
        balanceInfo[self.balanceKeys[balanceType]] = json.loads(balanceInfo[self.balanceKeys[balanceType]])
        return balanceInfo
    def get_last_ls_balance(self, botName, exchange):
        return self.get_last_balance(botName, exchange, 'long_short_balance')
    def get_last_borrow_balance(self, botName, exchange):
        return self.get_last_balance(botName, exchange, 'borrow_balance')
    def get_last_cushion_balance(self, botName, exchange):
        return self.get_last_balance(botName, exchange, 'cushion_balance')
    def get_last_reserve_currency_added(self, botName, exchange):
        return self.get_last_balance(botName, exchange, 'reserve_currency_added')


    def store_balance(self, botName, exchange, balanceType, balance, lastModificationTime, if_exists):
        assert balanceType in self.validBalanceTypes
        data = pd.DataFrame([{self.balanceKeys[balanceType]: json.dumps(balance), 'in_z': pd.Timestamp.now(), 'time': lastModificationTime}])
        data.to_sql(exchange+'_'+balanceType, self.DB_ENGINE, if_exists=if_exists, schema=botName, index=False)
    def store_ls_balance(self, botName, exchange, lsBalance, lastTradeTime):
        self.store_balance(botName, exchange, 'long_short_balance', lsBalance, lastTradeTime, 'append')
    def store_borrow_balance(self, botName, exchange, borrowBalance, lastWithdrawalDepositTime):
        self.store_balance(botName, exchange, 'borrow_balance', borrowBalance, lastWithdrawalDepositTime, 'append')
    def store_cushion_balance(self, botName, exchange, cushionBalance, lastTradeTime):
        self.store_balance(botName, exchange, 'cushion_balance', cushionBalance, lastTradeTime, 'append')
    def store_reserve_currency_added(self, botName, exchange, reserveCurrencyBalance, lastTradeTime):
        self.store_balance(botName, exchange, 'reserve_currency_added', reserveCurrencyBalance, lastTradeTime, 'append')

    def update_balances(self, botName, exchange, how, reserveCurrency,
                        lsBalances=None,
                        initialBorrowBalances=None,
                        cushionBalances=None,
                        reserveCurrencyAdded=None):

        balanceDicts = [lsBalances, initialBorrowBalances, cushionBalances, reserveCurrencyAdded]
        balanceDicts = [bd for bd in balanceDicts if bd is not None]
        if (how == 'replace' and len(balanceDicts) != 4):
            raise NameError('All balances must supplied on initialization.')
        elif (how == 'append' and len(balanceDicts) == 0):
            raise NameError('At least one balance type must be updated.')

        if(lsBalances is None):
            lsBalances = self.get_last_ls_balance(botName, exchange)['long_short_balance']
        if(initialBorrowBalances is None):
            initialBorrowBalances = self.get_last_borrow_balance(botName, exchange)['borrow_balance']
        if(cushionBalances is None):
            cushionBalances = self.get_last_cushion_balance(botName, exchange)['cushion_balance']

        actualBalances = self.get_balances()
        theoreticalMultipleUnitBalances = defaultdict(float)
        allCoins = set(lsBalances.keys() + initialBorrowBalances.keys() + cushionBalances.keys())

        relevantMultipleUnitActualBalances = {}
        for coin in allCoins:
            relevantMultipleUnitActualBalances[coin] = actualBalances[coin]['free']
            if(coin in lsBalances):
                theoreticalMultipleUnitBalances[coin]+=lsBalances[coin]
            if(coin in initialBorrowBalances):
                theoreticalMultipleUnitBalances[coin]+=initialBorrowBalances[coin]
            if(coin in cushionBalances):
                theoreticalMultipleUnitBalances[coin]+=cushionBalances[coin]

        tickers = self.get_tickers_safe()
        relevantSingleUnitActualBalances = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
                                                                                relevantMultipleUnitActualBalances,
                                                                                reserveCurrency, tickers)
        theoreticalSingleUnitBalances = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
                                                                                theoreticalMultipleUnitBalances,
                                                                                reserveCurrency, tickers)
        for coin in theoreticalMultipleUnitBalances:
            if((theoreticalSingleUnitBalances[coin]-relevantSingleUnitActualBalances[coin]) > 100):
                raise NameError('Invalid Balance:{}, Actual:{}, Theoretical:{}'.format(coin,
                                                                                       relevantSingleUnitActualBalances[coin],
                                                                                       theoreticalSingleUnitBalances[coin]))

        if (exchange == 'kraken'):
            lastTrades = run_function_safe(self.api.fetchMyTrades)
            lastTradeTime = max(
                [pd.Timestamp(trade['datetime']).tz_convert(None) for trade in lastTrades]) + pd.Timedelta(
                seconds=5)  # TODO try to figure out how to get last ledger
        else:
            lastTradeTime = pd.Timestamp.now()
        lastWithdrawalTime = pd.Timestamp.now()

        self.store_balance(botName, exchange, 'long_short_balance', lsBalances, lastTradeTime, how)
        self.store_balance(botName, exchange, 'borrow_balance', initialBorrowBalances, lastWithdrawalTime, how)
        self.store_balance(botName, exchange, 'cushion_balance', cushionBalances, lastTradeTime, how)
        if(reserveCurrencyAdded is not None):
            self.store_balance(botName, exchange, 'reserve_currency_added', reserveCurrencyAdded, lastTradeTime, how)


    def balance_tables_exists(self, botName, exchange):
        sqlSkeleton = "select exists( select 1 from information_schema.tables where table_schema = '{}' and table_name = '{}')"
        for balanceType in self.validBalanceTypes:
            exists = pd.read_sql(sqlSkeleton.format(botName, exchange + '_' + balanceType), self.DB_ENGINE).loc[0][0]
            if(not(exists)):
                return False
        return True

    def get_ls_positions_via_balances(self, botName, exchange, currencyUnit, idealSingleUnitLSBalance):
        allBalances = self.get_balances()
        multipleUnitBorrowBalanceInfo = self.get_last_borrow_balance(botName, exchange)
        multipleUnitCushionBalanceInfo = self.get_last_cushion_balance(botName, exchange)

        actualMultipleUnitLSBalance = {}
        for coin in idealSingleUnitLSBalance.keys():
            if(coin in multipleUnitCushionBalanceInfo['cushionBalance']):
                actualMultipleUnitLSBalance[coin] = (allBalances['free'][coin] -
                                                     multipleUnitBorrowBalanceInfo['borrowBalance'][coin] -
                                                     multipleUnitCushionBalanceInfo['cushionBalance'][coin])
            else:
                actualMultipleUnitLSBalance[coin] = (allBalances['free'][coin] -
                                                     multipleUnitBorrowBalanceInfo['borrowBalance'][coin])

        tickers = self.get_tickers_safe()
        actualSingleUnitLSBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            actualMultipleUnitLSBalance,
            currencyUnit,
            tickers)

        error = {}
        for coin in actualSingleUnitLSBalance:
            error[coin] = idealSingleUnitLSBalance[coin] - actualSingleUnitLSBalance[coin]

        return {'ideal': idealSingleUnitLSBalance, 'actual': actualSingleUnitLSBalance, 'diff': error}

    def get_ls_positions_via_db(self, botName, exchange, currencyUnit, idealSingleUnitLSBalance):
        actualMultipleUnitLSBalance = self.get_last_ls_balance(botName, exchange)['lsBalance']
        tickers = self.get_tickers_safe()
        actualSingleUnitLSBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            actualMultipleUnitLSBalance,
            currencyUnit,
            tickers)
        error = {}
        for coin in actualSingleUnitLSBalance:
            error[coin] = idealSingleUnitLSBalance[coin] - actualSingleUnitLSBalance[coin]

        return {'ideal': idealSingleUnitLSBalance, 'actual': actualSingleUnitLSBalance, 'diff': error}

    def get_new_balance_from_trades(self, initBalance, trades):
        endBalance = copy.deepcopy(initBalance)
        for trade in trades:
            #print(trade['symbol'], trade['cost'], trade['filled'], trade['fee'], trade['side'])
            base, quote = trade['symbol'].split('/')
            if (trade['side'] == 'sell'):
                if (base in endBalance):
                    endBalance[base] -= trade['filled']
                if (quote in endBalance):
                    endBalance[quote] += trade['cost']
            else:
                if (base in endBalance):
                    endBalance[base] += trade['filled']
                if (quote in endBalance):
                    endBalance[quote] -= trade['cost']

            if (trade['fee']['currency'] in endBalance):
                endBalance[trade['fee']['currency']] -= trade['fee']['cost']

        return endBalance
    def update_balance_via_trades(self,
                                  orders,
                                  commonCurrency,
                                  reserveCurrency,
                                  balance,
                                  lastTradeTime):

        # get trades from exchange if orders not given
        if (orders is None):
            # get market symbols
            marketsTradedOn = set()
            for instrument in balance:
                if ('{}/{}'.format(instrument, commonCurrency) in self.allMarketSyms):
                    marketsTradedOn.add('{}/{}'.format(instrument, commonCurrency))
                if ('{}/{}'.format(instrument, reserveCurrency) in self.allMarketSyms):
                    marketsTradedOn.add('{}/{}'.format(instrument, reserveCurrency))

            # start time
            endTime = pd.Timestamp.now() + pd.Timedelta(minutes=1)

            # historical trades
            orders = self.get_past_orders(lastTradeTime, endTime, marketsTradedOn,
                                          inclusive=False)
        #use given orders which occured after lastTradeTime
        else:
            validOrders = []
            for order in orders:
                tradeTime = pd.Timestamp(order['datetime'])
                if(tradeTime.tz is not None):
                    tradeTime = tradeTime.tz_convert(None)
                if(tradeTime > lastTradeTime):
                    validOrders.append(order)
            orders = validOrders

        if (len(orders) > 0):
            balance = self.get_new_balance_from_trades(balance,
                                                       orders)
            lastTradeTime = max([pd.Timestamp(order['datetime']) for order in orders])
            if (lastTradeTime.tz is not None):
                lastTradeTime = lastTradeTime.tz_convert(None)

        return balance, lastTradeTime, orders
    def update_ls_balance_via_trades(self, orders, commonCurrency, reserveCurrency, usingBNBForFees,
                                     actualStartMultipleUnitLSBalanceInfo):
        actualStartMultipleUnitLSBalance = actualStartMultipleUnitLSBalanceInfo['lsBalance']
        lastTradeTime = actualStartMultipleUnitLSBalanceInfo['time']
        self.log_it('info', 'Start LS Balance: {}'.format(str(actualStartMultipleUnitLSBalance)))

        if (orders is None and commonCurrency in actualStartMultipleUnitLSBalance):
            raise NameError('Can not recover via trades when common currency is being used as cushion and instrument.')
        if (orders is None and usingBNBForFees and 'BNB' in actualStartMultipleUnitLSBalance):
            raise NameError('Can not recover via trades when BNB is being used for fees and instrument.')

        newActualStartMultipleUnitLSBalance, newLastTradeTime, orders = self.update_balance_via_trades(orders,
                                  commonCurrency,
                                  reserveCurrency,
                                  actualStartMultipleUnitLSBalance,
                                  lastTradeTime)

        self.log_it('info', 'End LS Balance: {}'.format(str(newActualStartMultipleUnitLSBalance)))

        return newActualStartMultipleUnitLSBalance, newLastTradeTime, orders
    def update_cushion_balance_via_trades(self, orders, commonCurrency, reserveCurrency,
                                          actualStartMultipleUnitCushionBalanceInfo):
        self.log_it('info','==========Updating Cushion Via Trades==========')
        actualStartMultipleUnitCushionBalance = actualStartMultipleUnitCushionBalanceInfo['cushionBalance']
        lastTradeTime = actualStartMultipleUnitCushionBalanceInfo['time']
        self.log_it('info', 'Start Cushion: {}'.format(str(actualStartMultipleUnitCushionBalance)))
        newActualStartMultipleUnitCushionBalance, newLastTradeTime, orders = self.update_balance_via_trades(orders,
                                                                                               commonCurrency,
                                                                                               reserveCurrency,
                                                                                               actualStartMultipleUnitCushionBalance,
                                                                                               lastTradeTime)

        self.log_it('info', 'End Cushion: {}'.format(str(newActualStartMultipleUnitCushionBalance)))
        return newActualStartMultipleUnitCushionBalance, newLastTradeTime, orders

    def get_wds_since_last_borrow_balance_store(self, botName, exchange):
        borrowBalanceInfo = self.get_last_borrow_balance(botName, exchange)
        startTime = borrowBalanceInfo['time']
        endTime = pd.Timestamp.now() + pd.Timedelta(hours=1)  # some of kraken's ledger entries are in the future
        coinSymbols = borrowBalanceInfo['borrowBalances'].keys()
        return self.get_withdrawal_deposit_history(startTime, endTime, coinSymbols, inclusive=False)


    def update_borrow_balance_via_wds(self, botName, exchange, wds):
        borrowBalanceInfo = self.get_last_borrow_balance(botName, exchange)
        borrowBalances=borrowBalanceInfo['borrowBalances']
        lastWithdrawDepositTime=borrowBalanceInfo['time']
        if(len(wds)>0):
            for wd in wds:
                if(wd['datetime']>lastWithdrawDepositTime):
                    lastWithdrawDepositTime=wd['datetime']
                borrowBalances[wd['symbol']]+=wd['amount']-wd['fee']
            self.store_borrow_balance(botName, exchange, borrowBalances, lastWithdrawDepositTime)
        return borrowBalances

    def balance_changes_to_order_batches(self,
                                         balanceChanges,
                                         resub,
                                         commonCurrency,
                                         reserveCurrency):

        # convert to order objects/ separate orders
        ccBuys = []
        ccSells = []
        rcBuys = []
        rcSells = []
        ccBuyVolume = 0
        rcBuyVolume = 0
        for bc in balanceChanges:
            if (bc.suc > 0):
                side = 'buy'
            else:
                side = 'sell'
            if (bc.useRC):
                market = '{}/{}'.format(bc.instrument, reserveCurrency)
            else:
                market = '{}/{}'.format(bc.instrument, commonCurrency)

            amount = abs(bc.muc)
            initSizeSu = abs(bc.suc)
            order = OrderInfo(side, market, amount, initSizeSu,self, raiseMarketOfflineError=False) #TODO make this raiseMarketOfflineError configurable

            if (bc.useRC and bc.suc > 0):
                rcBuyVolume += bc.suc
                rcBuys.append(order)
            elif (bc.useRC and bc.suc < 0):
                rcSells.append(order)
            elif (not (bc.useRC) and bc.suc > 0):
                ccBuyVolume += bc.suc
                ccBuys.append(order)
            else:
                ccSells.append(order)

        firstBatch = []
        secondBatch = []
        if (len(rcBuys) > 0 and
                rcBuyVolume < resub[reserveCurrency] * .95):
            firstBatch += rcSells
            firstBatch += rcBuys
        else:
            firstBatch += rcSells
            secondBatch += rcBuys
        if (len(ccBuys) > 0 and
                ccBuyVolume < resub[commonCurrency] * .95):
            firstBatch += ccSells
            firstBatch += ccBuys
        else:
            firstBatch += ccSells
            secondBatch += ccBuys

        if (len(secondBatch) > 0):
            return [OrderBatch(firstBatch, self), OrderBatch(secondBatch, self)]
        else:
            return [OrderBatch(firstBatch, self)]

    def create_order_batches(self, ssub, smub, isub, imub, resub,
                             commonCurrency,
                             reserveCurrency,
                             marketSyms,
                             usingBNBForFees):
        instruments = isub.keys()

        class BalanceChange:
            def __init__(self, instrument, suc, muc):
                self.instrument = instrument
                self.suc = suc
                self.muc = muc
                self.useRC = False

        # get single/multi unit change (ideal-actual)
        balanceChanges = []
        for instrument in instruments:
            if (instrument != commonCurrency and not(instrument == 'BNB' and usingBNBForFees)):
                balanceChanges.append(BalanceChange(instrument,
                                                    isub.get(instrument) - ssub.get(instrument),
                                                    imub.get(instrument) - smub.get(instrument)))

        if (reserveCurrency == commonCurrency):
            return self.balance_changes_to_order_batches(
                balanceChanges,
                resub,
                commonCurrency,
                reserveCurrency)

        # try to make CC market volume net 0
        netCCVolume = 0
        for balanceChange in balanceChanges:
            if ('{}/{}'.format(balanceChange.instrument, reserveCurrency) in marketSyms):
                balanceChange.useRC = True
            else:
                netCCVolume += balanceChange.suc
        newSplitBalanceChange = None
        for balanceChange in balanceChanges:
            # if order is currently going to be executed on RC market and
            # executing it on the CC market would reduce netCC volume
            if (balanceChange.useRC and
                    abs(netCCVolume + balanceChange.suc) < abs(netCCVolume)):
                # check if order should be split up
                if (abs(netCCVolume) > abs(balanceChange.suc)):
                    netCCVolume += balanceChange.suc
                    balanceChange.useRC = False
                else:
                    # get % of order to be executed on CC market
                    ccPercent = abs(netCCVolume / balanceChange.suc)
                    # get % of order to be executed on RC market
                    rcPercent = 1 - ccPercent

                    balanceChange.useRC = False
                    balanceChange.suc *= ccPercent
                    balanceChange.muc *= ccPercent

                    newSplitBalanceChange = BalanceChange(balanceChange.instrument,
                                                          balanceChange.suc * rcPercent,
                                                          balanceChange.muc * rcPercent)
                    newSplitBalanceChange.useRC = True
        if (newSplitBalanceChange is not None):
            balanceChanges.append(newSplitBalanceChange)

        return self.balance_changes_to_order_batches(
            balanceChanges,
            resub,
            commonCurrency,
            reserveCurrency)

    def balance_bounds_adjustment(self,
                                  base,
                                  quote,
                                  bounds,
                                  tickers,
                                  cushionBalances,
                                  cushionAdjustmentOrderInfo,
                                  errors):

        if (cushionBalances[base] < bounds['min'] or
                cushionBalances[base] > bounds['max']):
            self.log_it('info', '==========Adjusting {} Balance=========='.format(base))
            if(bounds['min']==bounds['max']):
                self.log_it('info', 'Actual:{}, Ideal:{}'.format(cushionBalances[base],bounds['max']))
            else:
                self.log_it('info', 'Min:{}, Actual:{}, Max:{}'.format(bounds['min'],cushionBalances[base],bounds['max']))

            marketSym = '{}/{}'.format(base, quote)
            idealCushion = (bounds['min'] + bounds['max']) / 2.0
            initSizeSu = (idealCushion - cushionBalances[base])
            amount = initSizeSu / tickers[marketSym]['last']
            if (amount > 0):
                side = 'buy'
            else:
                side = 'sell'
            ob = OrderBatch([OrderInfo(side, marketSym, abs(amount), abs(initSizeSu), self, raiseMarketOfflineError=True)], self)
            results = ob.execute_batch()
            cushionAdjustmentOrderInfo += results['orderInfo']
            errors['dustOrder'] += results['errors']['dustOrder']
            errors['marketOffline'] += results['errors']['marketOffline']
            errors['sellingTooMuch'] += results['errors']['sellingTooMuch']
            errors['buyingTooMuch'] += results['errors']['buyingTooMuch']
        return cushionAdjustmentOrderInfo, errors

    def check_added_or_dropped_coins(self,
                                    botName,
                                    exchange,
                                    idealSingleUnitLSBalance):
        actualStartMultipleUnitLSBalanceInfo = self.get_last_ls_balance(botName, exchange)
        actualStartMultipleUnitLSBalance = actualStartMultipleUnitLSBalanceInfo['lsBalance']

        allCoins = set(idealSingleUnitLSBalance.keys()+actualStartMultipleUnitLSBalance.keys())

        addedCoin = False
        for coin in allCoins:
            #adding a coin
            if(coin in idealSingleUnitLSBalance and coin not in actualStartMultipleUnitLSBalance):
                self.log_it('info', 'Adding {} to portfolio.'.format(coin))
                actualStartMultipleUnitLSBalance[coin] = 0
                addedCoin = True
            elif(coin not in idealSingleUnitLSBalance and coin in actualStartMultipleUnitLSBalance):
                self.log_it('info', 'Dropping/dropped {} from portfolio.'.format(coin))
                idealSingleUnitLSBalance[coin] = 0

        #account for added coin
        if(addedCoin):
            self.store_ls_balance(botName,
                                  exchange,
                                  actualStartMultipleUnitLSBalance,
                                  actualStartMultipleUnitLSBalanceInfo['time'])

    def get_relevant_markets(self, portfolioCoins, commonCurrency, reserveCurrency, usingBNBForFees):
        relevantMarkets = set()
        for coin in portfolioCoins:
            if('{}/{}'.format(coin,commonCurrency) in self.allMarketSyms):
                relevantMarkets.add('{}/{}'.format(coin, commonCurrency))
            if ('{}/{}'.format(coin, reserveCurrency) in self.allMarketSyms):
                relevantMarkets.add('{}/{}'.format(coin, reserveCurrency))
        if('{}/{}'.format(commonCurrency, reserveCurrency) in self.allMarketSyms):
            relevantMarkets.add('{}/{}'.format(commonCurrency, reserveCurrency))
        if(usingBNBForFees):
            relevantMarkets.add('{}/{}'.format('BNB', reserveCurrency))
            relevantMarkets.add('{}/{}'.format('BNB', commonCurrency))
        return list(relevantMarkets)

    def recover_cushion_balance(self, coin, reserveCurrency, exchangeBalance, bounds, tickers, portfolioCoins):
        if(coin in portfolioCoins):
            minBal = self.routeFinder.convert_start_amoutn_to_end_amount(reserveCurrency, coin, bounds['min'], tickers)
            maxBal = self.routeFinder.convert_start_amoutn_to_end_amount(reserveCurrency, coin, bounds['max'], tickers)
            idealBal = (minBal+maxBal)/2
            if(exchangeBalance<idealBal):
                cushionBalance = exchangeBalance
            else:
                cushionBalance = idealBal
        else:
            cushionBalance = exchangeBalance

        cushionBalanceRC = self.routeFinder.convert_start_amoutn_to_end_amount(coin, reserveCurrency, cushionBalance,
                                                                               tickers)
        self.log_it('info', 'Recovered {} cushion balance. It will be assumed to be: {}, Min: {}, Max: {}'.format(coin,
                                                                        cushionBalanceRC, bounds['min'], bounds['max']))
        return cushionBalance

    def recover_via_exchange_balances(self,
                                      relevantMarkets,
                                      portfolioCoins,
                                      commonCurrency,
                                      reserveCurrency,
                                      borrowBalanceInfo,
                                      usingBNBForFees,
                                      BNBBounds,
                                      cushionBounds,
                                      botName,
                                      exchange,
                                      storeInDB=True):



        self.log_it('info', '==========Recovering LS Balance Via Exchange Balances==========')
        #get relevant coins
        relevantCoins = set(portfolioCoins)
        relevantCoins.add(commonCurrency)
        if (usingBNBForFees):
            relevantCoins.add('BNB')

        # check for withdrawals/deposits which may have altered borrow balance
        wds = self.get_withdrawal_deposit_history(borrowBalanceInfo['time'],
                                                  pd.Timestamp.now() + pd.Timedelta(hours=1),
                                                  relevantCoins,
                                                  inclusive=False)
        if (len(wds) > 0):
            wdsString = 'Review withdrawals and choose which ones to update borrow balances with via the store_borrow_balance function.\n'
            for wd in wds:
                wdsString += str(wd) + '\n'
            raise NameError(wdsString)

        borrowBalances = borrowBalanceInfo['borrowBalance']
        actualExchangeBalances = self.get_balances()

        #recover cushion balances where necessary
        cushionBalances = {}
        tickers = self.get_tickers_safe(relevantMarkets)
        if(commonCurrency != reserveCurrency):
            cushionBalances[commonCurrency] = self.recover_cushion_balance(commonCurrency, reserveCurrency,
                                                                           actualExchangeBalances[commonCurrency]['free'],
                                                                           cushionBounds, tickers, portfolioCoins)
        if(usingBNBForFees):
            cushionBalances['BNB'] = self.recover_cushion_balance('BNB', reserveCurrency,
                                                                           actualExchangeBalances['BNB']['free'],
                                                                           BNBBounds, tickers, portfolioCoins)

        #recover long/short balances
        actualStartMultipleUnitLSBalance = {}
        for coin in portfolioCoins:
            if(coin in cushionBalances):
                actualStartMultipleUnitLSBalance[coin] = actualExchangeBalances[coin]['free'] - borrowBalances[coin] - cushionBalances[coin]
            else:
                actualStartMultipleUnitLSBalance[coin] = actualExchangeBalances[coin]['free'] - borrowBalances[coin]

        #update db
        if(storeInDB):
            self.store_ls_balance(botName, exchange, actualStartMultipleUnitLSBalance, pd.Timestamp.now())
            self.store_cushion_balance(botName, exchange, cushionBalances, pd.Timestamp.now())

        return actualStartMultipleUnitLSBalance, cushionBalances

    def adjust_cushion_coin(self, coin, reserveCurrency, imulsb, amulsb, cushionBalance, bounds, tickers, orderInfo, errors):
        self.log_it('info', '==========Adjust Cushion Coin {}=========='.format(coin))
        changeLSB = 0
        if(coin in imulsb):
            changeLSB = imulsb[coin] - amulsb[coin]

        minC = self.routeFinder.convert_start_amoutn_to_end_amount(reserveCurrency,coin, bounds['min'], tickers)
        maxC = self.routeFinder.convert_start_amoutn_to_end_amount(reserveCurrency,coin, bounds['max'], tickers)
        changeCB = 0
        if(minC > cushionBalance or cushionBalance > maxC):
            self.log_it('info','Adjusting Cushion Balance. Min:{}, Actual:{}, Max:{}'.format(minC, cushionBalance, maxC))
            changeCB = ((minC+maxC)/2) - cushionBalance

        overallChange = changeLSB+changeCB

        self.log_it('info', 'Cushion Change:{}, Long/Short Change:{}, Overall Change:{}'.format(changeCB, changeLSB, overallChange))

        lastTradeTime = None

        cushionChange = 0
        if(overallChange != 0):
            initSizeSu = self.routeFinder.convert_start_amoutn_to_end_amount(coin, reserveCurrency, overallChange, tickers)
            if(overallChange<0):
                side = 'sell'
                overallChange*=-1
                initSizeSu*=-1
            else:
                side = 'buy'

            order = OrderInfo(side,'{}/{}'.format(coin, reserveCurrency),overallChange, initSizeSu, self)
            orderBatch = OrderBatch([order], self, tryFreq=8)
            results = orderBatch.execute_batch()
            errors['sellingTooMuch'] += results['errors']['sellingTooMuch']
            errors['buyingTooMuch'] += results['errors']['buyingTooMuch']
            errors['dustOrder'] += results['errors']['dustOrder']
            errors['marketOffline'] += results['errors']['marketOffline']

            if(len(results['orderInfo'])):
                lastTradeTime = max([pd.Timestamp(order['datetime']) for order in results['orderInfo']])

            actualChange = self.get_new_balance_from_trades({coin: 0}, results['orderInfo'])[coin]
            cushionChange = actualChange - changeLSB

            orderInfo += results['orderInfo']
        if(coin in imulsb):
            return imulsb[coin], cushionBalance+cushionChange, lastTradeTime
        else:
            return None, cushionBalance+cushionChange,  lastTradeTime

    def rebalance(self,
                  botName,
                  exchange,
                  idealSingleUnitLSBalance,
                  commonCurrency,
                  reserveCurrency,
                  cushionBounds,
                  usingBNBForFees,
                  BNBBounds,
                  altSimPriceData,
                  mode,
                  sanityCheck,
                  plannedFail):

        self.log_it('info', '==========START REBALANCE==========')
        if(sanityCheck):
            self.log_it('info', '==========SANITY CHECK==========')

        #basic functionality checks
        assert mode in ['normal', 'recoverViaExchangeTrades', 'recoverViaExchangeBalances']
        if(commonCurrency != reserveCurrency and
                mode == 'recoverViaExchangeTrades' and
                commonCurrency in idealSingleUnitLSBalance):
            raise NameError('Can not recover via trades when common currency is being used as cushion and instrument.')
        if(usingBNBForFees and 'BNB' in idealSingleUnitLSBalance and mode == 'recoverViaExchangeTrades'):
            raise NameError('Can not recover via trades when BNB is being used for fees and instrument.')
        if(not(self.balance_tables_exists(botName, exchange))):
            raise NameError('Balances not instanciated.')

        relevantMarkets = self.get_relevant_markets(idealSingleUnitLSBalance.keys(), commonCurrency, reserveCurrency, usingBNBForFees)
        borrowBalanceInfo = self.get_last_borrow_balance(botName, exchange)


        self.log_it('info', '==========Check Added/Dropped Coins==========')

        self.check_added_or_dropped_coins(botName,
                                         exchange,
                                         idealSingleUnitLSBalance)

        # order info for cushion adjustments/ bnb adjustments/ rebalancing
        cushionAdjustmentOrderInfo = []
        bnbAdjustmentOrderInfo = []
        rebalanceOrderInfo = []
        recoveredCushionAdjustmentOrderInfo = []
        recoveredBNBAdjustmentOrderInfo = []
        recoveredRebalanceOrderInfo = []
        #all errors
        errors = {'sellingTooMuch': [],
                  'buyingTooMuch': [],
                  'dustOrder': [],
                  'marketOffline': []}
        #multiple/single unit balances after each major "rebalancing event"
        balanceHistory=[]

        #get actual long/short and cushion balances
        if(mode == 'recoverViaExchangeTrades'):
            self.log_it('info', '==========Recovering LS Balance Via Exchange Trades==========')
            #pull rebalance trades from exchange
            actualStartMultipleUnitLSBalancInfo = self.get_last_ls_balance(botName, exchange)
            actualStartMultipleUnitLSBalance,lastLSTradeTime,recoveredRebalanceOrderInfo = self.update_ls_balance_via_trades(None,
                                                                              commonCurrency,
                                                                              reserveCurrency,
                                                                              usingBNBForFees,
                                                                              actualStartMultipleUnitLSBalancInfo)
            #pull cushion adjustment trades from exchange
            startMultipleUnitCushionBalanceInfo = self.get_last_cushion_balance(botName, exchange)
            startMultipleUnitCushionBalance,lastCushionTradeTime,recoveredCushionOrderInfo = self.update_cushion_balance_via_trades(None,
                                              commonCurrency,
                                              reserveCurrency,
                                              startMultipleUnitCushionBalanceInfo)
            startMultipleUnitCushionBalanceInfo['cushionBalance'] = startMultipleUnitCushionBalance
            #update cushion with trades which indirectly affected it
            startMultipleUnitCushionBalance, _, _ = self.update_cushion_balance_via_trades(recoveredRebalanceOrderInfo,
                                             commonCurrency,
                                             reserveCurrency,
                                             startMultipleUnitCushionBalanceInfo)
            lastCushionTradeTime = max([lastCushionTradeTime, lastLSTradeTime])
            #store cushion before ls balance in case need to get all trades since last ls trade time to update
            #cushion (ie: if failure occurs after ls balance store but before cushion balance store)
            self.store_cushion_balance(botName, exchange, startMultipleUnitCushionBalance, lastCushionTradeTime)
            self.store_ls_balance(botName, exchange, actualStartMultipleUnitLSBalance, lastLSTradeTime)

            #separate BNB and CC cushion orders
            for order in recoveredCushionOrderInfo:
                if(order['symbol'].split('/')=='BNB'):
                    recoveredBNBAdjustmentOrderInfo.append(order)
                else:
                    recoveredCushionAdjustmentOrderInfo.append(order)

        elif(mode == 'recoverViaExchangeBalances'):
            actualStartMultipleUnitLSBalance,startMultipleUnitCushionBalance=self.recover_via_exchange_balances(relevantMarkets,
                                          idealSingleUnitLSBalance.keys(),
                                          commonCurrency,
                                          reserveCurrency,
                                          borrowBalanceInfo,
                                          usingBNBForFees,
                                          BNBBounds,
                                          cushionBounds,
                                          botName,
                                          exchange)
        elif(mode == 'normal'):
            self.log_it('info', '==========Fetching LS Balance From DB==========')
            actualStartMultipleUnitLSBalanceInfo = self.get_last_ls_balance(botName, exchange)
            actualStartMultipleUnitLSBalance = actualStartMultipleUnitLSBalanceInfo['lsBalance']
            actualStartMultipleUnitCushionBalanceInfo = self.get_last_cushion_balance(botName, exchange)
            startMultipleUnitCushionBalance = actualStartMultipleUnitCushionBalanceInfo['cushionBalance']
        else:
            raise NameError('Invalid mode: {}'.format(mode))

        # unit conversions
        tickers = self.get_tickers_safe(relevantMarkets)

        idealMultipleUnitLSBalance = self.routeFinder.convert_single_unit_portfolio_to_multiple_unit_portfolio(
            idealSingleUnitLSBalance, reserveCurrency, tickers)
        actualStartSingleUnitLSBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            actualStartMultipleUnitLSBalance, reserveCurrency, tickers)
        startSingleUnitCushionBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            startMultipleUnitCushionBalance, reserveCurrency, tickers)
        self.log_it('info','Actual LS: '+str(actualStartSingleUnitLSBalance))
        self.log_it('info','Ideal LS: '+str(idealSingleUnitLSBalance))
        self.log_it('info', 'Cushion Balances: '+str(startSingleUnitCushionBalance))

        # get actual exchange balances
        self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers, usingBNBForFees)
        balanceHistory.append({'su':self.relevantSingleUnitBalances, 'mu':self.relevantMultipleUnitBalances})

        # create order batches
        orderBatches = self.create_order_batches(actualStartSingleUnitLSBalance,
                                                 actualStartMultipleUnitLSBalance,
                                                 idealSingleUnitLSBalance,
                                                 idealMultipleUnitLSBalance,
                                                 self.relevantSingleUnitBalances,
                                                 commonCurrency,
                                                 reserveCurrency,
                                                 set(tickers.keys()),
                                                 usingBNBForFees)
        self.log_it('info','==========Calculated Order Batches==========')
        for i,orderBatch in enumerate(orderBatches):
            self.log_it('info','Batch:{}'.format(i))
            orderBatch.view_batch()

        if(sanityCheck):
            return None

        # adjust BNB if using for fees
        if(usingBNBForFees):
            bnbAdjustmentOrderInfo, errors = self.balance_bounds_adjustment(
                                      'BNB',
                                      reserveCurrency,
                                      BNBBounds,
                                      tickers,
                                      startSingleUnitCushionBalance,
                                      bnbAdjustmentOrderInfo,
                                      errors)

            tickers = self.get_tickers_safe(relevantMarkets)
            self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers,
                                          usingBNBForFees)

        balanceHistory.append({'su':self.relevantSingleUnitBalances, 'mu':self.relevantMultipleUnitBalances})

        # check if net buy volume on CC markets > cushion balance
        # TODO account for case when moving RC to CC prevents RC trades from being made
        if (commonCurrency != reserveCurrency):

            #get net buy volume
            netVolume = 0
            for orderBatch in orderBatches:
                netVolume += orderBatch.get_net_volume(commonCurrency)

            if (netVolume*1.05 > self.relevantSingleUnitBalances[commonCurrency]):
                self.log_it('info', 'Temporarily need more {}. Net Rebalance Volume: {}, Actual Balance:{}'.format(commonCurrency,
                                                                                                     netVolume,
                                                                                                  self.relevantSingleUnitBalances[commonCurrency]))
                dummyBounds={'min':netVolume*1.05,'max':netVolume*1.05}
                cushionAdjustmentOrderInfo, errors = self.balance_bounds_adjustment(
                    commonCurrency,
                    reserveCurrency,
                    dummyBounds,
                    tickers,
                    startSingleUnitCushionBalance,
                    cushionAdjustmentOrderInfo,
                    errors)

            #adjust cushion normally
            else:
                if(netVolume*1.05 < (cushionBounds['max']+cushionBounds['min'])/2):
                    cushionAdjustmentOrderInfo, errors = self.balance_bounds_adjustment(
                        commonCurrency,
                        reserveCurrency,
                        cushionBounds,
                        tickers,
                        startSingleUnitCushionBalance,
                        cushionAdjustmentOrderInfo,
                        errors)

            tickers = self.get_tickers_safe(relevantMarkets)
            self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers,
                                          usingBNBForFees)

        balanceHistory.append({'su':self.relevantSingleUnitBalances, 'mu':self.relevantMultipleUnitBalances})

        # execute orders TODO adjust cushion while rebalancing
        for i,orderBatch in enumerate(orderBatches):
            self.log_it('info', '==========Starting Rebalance Batch {}=========='.format(i))
            results = orderBatch.execute_batch()
            rebalanceOrderInfo += results['orderInfo']
            errors['sellingTooMuch'] += results['errors']['sellingTooMuch']
            errors['buyingTooMuch'] += results['errors']['buyingTooMuch']
            errors['dustOrder'] += results['errors']['dustOrder']
            errors['marketOffline'] += results['errors']['marketOffline']

        tickers = self.get_tickers_safe(relevantMarkets)
        self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers,
                                      usingBNBForFees)
        balanceHistory.append({'su':self.relevantSingleUnitBalances, 'mu':self.relevantMultipleUnitBalances})

        #get new cushion balance
        potentialCushionOrders = rebalanceOrderInfo+bnbAdjustmentOrderInfo+cushionAdjustmentOrderInfo
        startMultipleUnitCushionBalanceInfo = self.get_last_cushion_balance(botName, exchange)
        endMultipleUnitCushionBalance, lastCushionTradeTime, _ = self.update_cushion_balance_via_trades(
                                                                            potentialCushionOrders,
                                                                            commonCurrency,
                                                                            reserveCurrency,
                                                                            startMultipleUnitCushionBalanceInfo)

        #Note: ls balances assets which also act as cushions are overwritten by adjust_cushion_coin
        actualStartMultipleUnitLSBalanceInfo = self.get_last_ls_balance(botName, exchange)
        actualEndMultipleUnitLSBalance, lastLSTradeTime, _ = self.update_ls_balance_via_trades(rebalanceOrderInfo,
                                                                           commonCurrency,
                                                                           reserveCurrency,
                                                                           usingBNBForFees,
                                                                            actualStartMultipleUnitLSBalanceInfo)

        if (commonCurrency != reserveCurrency):
            lsBalance, cushionBalance, lastTradeTime = self.adjust_cushion_coin(commonCurrency,
                                                                 reserveCurrency,
                                                                 idealMultipleUnitLSBalance,
                                                                 actualStartMultipleUnitLSBalance,
                                                                 endMultipleUnitCushionBalance[commonCurrency],
                                                                 cushionBounds,
                                                                 tickers,
                                                                 cushionAdjustmentOrderInfo,
                                                                 errors)
            if(lsBalance is not None):
                actualEndMultipleUnitLSBalance[commonCurrency] = lsBalance
            endMultipleUnitCushionBalance[commonCurrency] = cushionBalance
            if(lastTradeTime is not None):
                lastLSTradeTime = lastTradeTime
                lastCushionTradeTime = lastTradeTime
            self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers,
                                      usingBNBForFees)

        balanceHistory.append({'su': self.relevantSingleUnitBalances, 'mu': self.relevantMultipleUnitBalances})

        if (usingBNBForFees):
            tickers = self.get_tickers_safe(relevantMarkets)
            lsBalance, cushionBalance, lastTradeTime = self.adjust_cushion_coin('BNB',
                                                                 reserveCurrency,
                                                                 idealMultipleUnitLSBalance,
                                                                 actualStartMultipleUnitLSBalance,
                                                                 endMultipleUnitCushionBalance['BNB'],
                                                                 BNBBounds,
                                                                 tickers,
                                                                 bnbAdjustmentOrderInfo,
                                                                 errors)
            if (lsBalance is not None):
                actualEndMultipleUnitLSBalance['BNB'] = lsBalance
            endMultipleUnitCushionBalance['BNB'] = cushionBalance
            if (lastTradeTime is not None):
                lastLSTradeTime = lastTradeTime
                lastCushionTradeTime = lastTradeTime
            self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers,
                                          usingBNBForFees)
        balanceHistory.append({'su': self.relevantSingleUnitBalances, 'mu': self.relevantMultipleUnitBalances})

        #store long/short and cushion balances
        if(plannedFail):
            raise NameError('Planned Fail')
        else:
            self.store_ls_balance(botName, exchange, actualEndMultipleUnitLSBalance, lastLSTradeTime)
            self.store_cushion_balance(botName, exchange, endMultipleUnitCushionBalance, lastCushionTradeTime)

        #unit conversions
        actualEndSingleUnitLSBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            actualEndMultipleUnitLSBalance, reserveCurrency, tickers)
        endSingleUnitCushionBalance = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            endMultipleUnitCushionBalance, reserveCurrency, tickers)

        tickers = self.get_tickers_safe(relevantMarkets)
        self.update_relevant_balances(commonCurrency, reserveCurrency, idealSingleUnitLSBalance, tickers,
                                      usingBNBForFees)
        balanceHistory.append({'su':self.relevantSingleUnitBalances, 'mu':self.relevantMultipleUnitBalances})

        self.log_it('info', '==========END REBALANCE==========')

        return {'balanceHistory':balanceHistory,
                'bnbAdjustmentOrderInfo': bnbAdjustmentOrderInfo,
                'rebalanceOrderInfo': rebalanceOrderInfo,
                'cushionAdjustmentOrderInfo': cushionAdjustmentOrderInfo,
                'recoveredBNBAdjustmentOrderInfo': recoveredBNBAdjustmentOrderInfo,
                'recoveredRebalanceOrderInfo': recoveredRebalanceOrderInfo,
                'recoveredCushionAdjustmentOrderInfo': recoveredCushionAdjustmentOrderInfo,
                'errors': errors,
                'idealSingleUnitLSBalance': idealSingleUnitLSBalance,
                'idealMultipleUnitLSBalance': idealMultipleUnitLSBalance,
                'actualStartSingleUnitLSBalance': actualStartSingleUnitLSBalance,
                'actualStartMultipleUnitLSBalance': actualStartMultipleUnitLSBalance,
                'actualEndSingleUnitLSBalance': actualEndSingleUnitLSBalance,
                'actualEndMultipleUnitLSBalance': actualEndMultipleUnitLSBalance,
                'startSingleUnitCushionBalance': startSingleUnitCushionBalance,
                'startMultipleUnitCushionBalance': startMultipleUnitCushionBalance,
                'endSingleUnitCushionBalance': endSingleUnitCushionBalance,
                'endMultipleUnitCushionBalance': endMultipleUnitCushionBalance,
                'altSimPriceData': altSimPriceData}

    def cancel_order_and_return_info(self, orderInfo):
        try:
            if('symbol' in orderInfo):
                self.cancel_order(self.get_order_id(orderInfo), orderInfo['symbol'])
            else:
                self.cancel_order(self.get_order_id(orderInfo))
        except ccxt.InvalidOrder as e:
            if (not (self.is_order_not_open_error(e))):
                raise e
        time.sleep(.1)
        stillOpen=True
        while(stillOpen):
            if('symbol' in orderInfo):
                orderInfo = self.get_order_stats(self.get_order_id(orderInfo), marketSym=orderInfo['symbol'])
            else:
                orderInfo = self.get_order_stats(self.get_order_id(orderInfo))
            stillOpen = 'open'==orderInfo['status']

        if (hasattr(self, 'get_real_order_info')):
            orderInfo = self.get_real_order_info(orderInfo)

        return orderInfo

    def check_if_order_went_through(self, orderInfo, tries=60, sleepTime=2, raisePartialFillError=True):
        self.log_it('info', 'Checking if order went through...')
        time.sleep(sleepTime)
        for i in range(tries):
            if('symbol' in orderInfo):
                orderInfo = self.get_order_stats(self.get_order_id(orderInfo), marketSym=orderInfo['symbol'])
            else:
                orderInfo = self.get_order_stats(self.get_order_id(orderInfo))
            self.log_it('info',
                        '{} remaining: {}, filled: {}'.format(orderInfo['symbol'].split('/')[0], orderInfo['remaining'],
                                                              orderInfo['filled']))
            if (not (orderInfo['status'] == 'open')):
                if (hasattr(self, 'get_real_order_info')):
                    orderInfo = self.get_real_order_info(orderInfo)
                return orderInfo
            time.sleep(sleepTime)

        # cancel order if its not being filled
        orderInfo = self.cancel_order_and_return_info(orderInfo)

        if (orderInfo['remaining'] == 0):
            return orderInfo

        if (raisePartialFillError):
            raise NameError(str(self.get_order_id(orderInfo)) + ' NOT Filled!')

        return orderInfo

    def get_order_id(self, order):
        raise NotImplementedError("You need to implement this")

    def cancel_order(self, orderId, symbol):
        raise NotImplementedError("You need to implement this")

    def choose_route(self, allRouts):
        raise NotImplementedError("You need to implement this")

    def get_balances(self):
        raise NotImplementedError("You need to implement this")

    def limit_buy(self, market, amount, highest_rate):
        raise NotImplementedError("You need to implement this")

    def market_buy(self, market, amount, highest_rate):
        raise NotImplementedError("You need to implement this")

    def limit_sell(self, market, amount, lowest_rate):
        raise NotImplementedError("You need to implement this")

    def market_sell(self, market, amount, lowest_rate):
        raise NotImplementedError("You need to implement this")

    def get_ob(self, market, depth=100):
        raise NotImplementedError("You need to implement this")

    def format_ob(self, ob):
        raise NotImplementedError("You need to implement this")

    def get_order_stats(self, order_id, marketSym=None, timestamp=None):
        raise NotImplementedError("You need to implement this")

    def get_fees(self, pair):
        raise NotImplementedError("You need to implement this")

    def withdraw(self, exchange_to):
        raise NotImplementedError("You need to implement this")