from utils.RouteFinder import RouteFinder
from utils.postgresConnection import  query, table_exists, get_tables, storeInDb
import json, os, math, shutil
import ccxt
import pandas as pd
from collections import defaultdict
from Exchanges.Binance import Binance
from Exchanges.Kraken import Kraken
from utils.funcs import run_function_safe
#workspace computers are windows
if os.name == 'nt':
    import matplotlib.pyplot as plt
    from matplotlib.pylab import savefig
import numpy as np

class OMSDataAnalytics:
    def __init__(self, botName, omsDataEnvironment='aws_dev',exchangeDataEnvironment='aws_exchanges'):
        self.botName = botName
        self.order_functions = ['smart_order', 'rebalance']

        self.borrowBalancesTableName = '_borrow_balance'
        self.reserveCurrencyAddedTableName = '_reserve_currency_added'
        self.validExchanges = ['kraken','binance']

        self.omsDataEnvironment=omsDataEnvironment
        self.exchangeDataEnvironment = exchangeDataEnvironment
        self.reserveCurrencies = {'binance':'USDT','kraken':'USD'}

        self.OMS_2_ALTSIM_EXCHANGE = {'bittrex':'BTRX', 'kraken':'KRKN'}
        self.EXCHANGE_TEATHER_CONVERSION = {'bittrex':{'USD':'USDT'}}
        self.routeFinders = {}

        analyticsDir, _ = os.path.split(os.path.abspath(__file__))
        self.botstatsFolder = os.path.join(analyticsDir, 'botstats', botName)


    def init_route_finders(self, exchanges):
        for exchange in exchanges:
            if(exchange not in self.routeFinders):
                marketSymbols = run_function_safe(getattr(ccxt, exchange)().fetch_tickers).keys()
                self.routeFinders[exchange] = RouteFinder(exchange, marketSymbols)

    def get_end_per_unit_start_quote(self, exchange, startQuote, endQuote, time, ohlcvQuoteData):
        if(exchange not in self.routeFinders):
            self.init_route_finders([exchange])
        routeFinder = self.routeFinders[exchange]
        route = routeFinder.find_shortest_rout(startQuote, endQuote)

        if('{}/{}'.format(startQuote,endQuote) in ohlcvQuoteData):
            market = '{}/{}'.format(startQuote,endQuote)
        else:
            market = '{}/{}'.format(endQuote,startQuote)

        time = time.round('1min')
        try:
            tickerDict = {market:{'last':ohlcvQuoteData[market]['close'][time]}}
        except:
            print(ohlcvQuoteData[market]['close'][time-pd.Timedelta(minutes=5):time+pd.Timedelta(minutes=5)].resample('1T').mean().interpolate())
            input()

        return routeFinder.get_end_per_unit_start_currency(route, tickerDict)

    def convert_to_teather(self, exchange, currency):
        if(exchange in self.EXCHANGE_TEATHER_CONVERSION and
           currency in self.EXCHANGE_TEATHER_CONVERSION[exchange]):
            return self.EXCHANGE_TEATHER_CONVERSION[exchange][currency]
        else:
            return currency

    def get_past_price_data(self, exchange, market, startDate, endDate):
        schema = self.OMS_2_ALTSIM_EXCHANGE[exchange]
        b,q=market.split('/')
        tableName = 'OHLCV_SPOT_{}_{}'.format(b,q)
        sql = """select * from "{}"."{}" where time >= '{}' and time <= '{}' order by time asc""".format(schema,tableName,str(startDate),str(endDate))
        ohlcvData = query(sql,environment=self.exchangeDataEnvironment, dataframe=True)
        ohlcvData = ohlcvData.set_index('time')
        ohlcvData = ohlcvData.resample('1T').mean().interpolate()
        return ohlcvData

    def get_past_price_data(self, exchange, startDate, endDate):
        markets = run_function_safe(getattr(ccxt,exchange)().fetch_tickers).keys()
        quoteCurrencies =  set([])
        for market in markets:
            quoteCurrencies.add(market.split('/')[1])
        allOhlcvData = {}
        for market in markets:
            b,q= market.split('/')
            if(b in quoteCurrencies and q in quoteCurrencies):
                allOhlcvData[market]=self.get_past_price_data(exchange, market, startDate, endDate)
        return allOhlcvData

    def get_exchanges_traded_on(self):
        tables = get_tables(self.botName, environment=self.omsDataEnvironment)
        exchanges = []
        for table in tables:
            if(self.borrowBalancesTableName in table and table.split('_')[0] in self.validExchanges):
                exchanges.append(table.split('_')[0])
        return exchanges

    def get_borrow_balances(self, exchange):
        tn = exchange+self.borrowBalancesTableName
        sql = 'select * from "{}".{} order by in_z {}'.format(self.botName, tn, 'asc')
        borrowBalancesDF = query(sql, environment=self.omsDataEnvironment, dataframe=True)
        borrowBalancesDF = borrowBalancesDF.set_index('time')['borrowBalance'].apply(json.loads)
        return borrowBalancesDF
    def get_reserve_currency_added(self, exchange):
        #fetch data
        tn = exchange + self.reserveCurrencyAddedTableName
        sql = 'select * from "{}".{} order by in_z {}'.format(self.botName, tn, 'asc')
        rcAddedDF = query(sql, environment=self.omsDataEnvironment, dataframe=True)

        #reformat data
        if(len(rcAddedDF)>0):
            rcAddedDF = rcAddedDF.set_index('time')
            def rcDict2rcNum(jsonStr):
                dict = json.loads(jsonStr)
                for cur in dict:
                    return dict[cur]
                raise NameError('Reserve currency added dict can not be empty.')
            rcAddedDF['reserveCurrencyAdded'] = rcAddedDF['reserveCurrencyAdded'].apply(rcDict2rcNum)

        return rcAddedDF

    def get_exchange_apis(self, exchanges):
        def get_key_path(exchange):
            if(os.path.isfile(exchange+'_analytics.key')):
                return exchange+'_analytics.key'
            else:
                raise NameError('{} key path not found!'.format(exchange))

        exchangeApis = {}
        for exchange in exchanges:
            if(exchange == 'binance'):
                exchangeApis[exchange] = Binance(get_key_path(exchange))
            elif(exchange == 'kraken'):
                exchangeApis[exchange] = Kraken(get_key_path(exchange))
            else:
                raise NameError('Exchange: {} not supported!'.format(exchange))
        return exchangeApis

    def get_first_stored_balances_from_rebalance(self, exchanges, startTime):
        initExchangeBalances={}
        for exchange in exchanges:
            sql = 'select * from {}.rebalance where in_z <= \'{}\' and exchange = \'{}\' order by in_z desc limit 1'.format(self.botName,
                                                                                        startTime,
                                                                                        exchange)
            df = query(sql,environment=self.omsDataEnvironment,dataframe=True)
            rebalanceInfo=json.loads(df.iloc[-1].to_dict()['result'])
            initBalance=rebalanceInfo['result']['balanceHistory'][0]
            initExchangeBalances[exchange]=initBalance

        return initExchangeBalances

    def get_current_exchange_balances(self, exchangeApis, relevantCoins):
        self.init_route_finders(exchangeApis.keys())
        curExchangeBalances={}
        for exchange in exchangeApis:
            curBalances = exchangeApis[exchange].get_balances()
            relevantMuBalances = {}
            for coin in relevantCoins[exchange]:
                relevantMuBalances[coin] = curBalances[coin]['free']
            relevantSuBalances = self.routeFinders[exchange].convert_multiple_unit_portfolio_to_single_unit_portfolio(
                relevantMuBalances,
                self.reserveCurrencies[exchange],
                run_function_safe(getattr(ccxt, exchange)().fetch_tickers))
            curExchangeBalances[exchange] = {'mu':relevantMuBalances,'su':relevantSuBalances}
        return curExchangeBalances

    def get_tickers_from_mu_and_su_balances(self, balances):
        coins=balances['su'].keys()
        tickers = {}
        for coin in coins:
            if(not(balances['mu'][coin])):
                tickers[coin]=0
            else:
                tickers[coin]=balances['su'][coin]/balances['mu'][coin]
        return tickers

    def get_portfolio_value(self, exchangeBalances, borrowBalances, tickers):
        totalValue=0
        for coin in exchangeBalances:
            totalValue+=(exchangeBalances[coin]-borrowBalances[coin])*tickers[coin]
        return totalValue

    def get_pnl(self, initExchangeBalances, initBorrowBalances, initTickers,
                finalExchangeBalances, finalBorrowBalances, finalTickers, rcAdded):
        exchanges = initExchangeBalances.keys()

        combined=0
        pnlInfo={'init':{},'final':{},'net':{}}
        for exchange in exchanges:
            exchangeInitValue = self.get_portfolio_value(initExchangeBalances[exchange],
                                                         initBorrowBalances[exchange],
                                                         initTickers[exchange])
            pnlInfo['init'][exchange]=exchangeInitValue
            exchangeFinalValue = self.get_portfolio_value(finalExchangeBalances[exchange],
                                                         finalBorrowBalances[exchange],
                                                         finalTickers[exchange])

            pnlInfo['final'][exchange]=exchangeFinalValue-rcAdded[exchange]
            net=exchangeFinalValue-exchangeInitValue-rcAdded[exchange]
            pnlInfo['net'][exchange]=net
            combined+=net
        pnlInfo['combined']=combined
        return pnlInfo

    def get_current_pnl(self, startTime):
        # get initial borrow balances
        exchanges = self.get_exchanges_traded_on()
        initBorrowBalances = {}
        reserveCurrencyAdded = {}
        curBorrowBalances = {}
        for exchange in exchanges:
            borrowBalances = self.get_borrow_balances(exchange)
            if(borrowBalances.index[0] > startTime):
                raise NameError('Borrow balances were not known before {}.'.format(borrowBalances.index[0]))

            rca = self.get_reserve_currency_added(exchange)
            if(len(rca)>0):
                reserveCurrencyAdded[exchange] = rca[rca.index >= startTime].sum().iloc[0]
            else:
                reserveCurrencyAdded[exchange] = 0

            #borrow balance at start time
            initBorrowBalances[exchange] = borrowBalances[borrowBalances.index <= startTime].iloc[-1]
            #current borrow balance
            curBorrowBalances[exchange] = borrowBalances.iloc[-1]

        # get initial exchange balances
        initExchangeBalances = self.get_first_stored_balances_from_rebalance(exchanges, startTime)

        # get current exchange balances
        relevantCoins = {}
        for exchange in initExchangeBalances:
            relevantCoins[exchange] = initExchangeBalances[exchange]['su'].keys()
        exchangeApis = self.get_exchange_apis(exchanges)
        curExchangeBalances = self.get_current_exchange_balances(exchangeApis, relevantCoins)

        #get tickers from single/multiple unit exchange balances
        initTickers = {}
        curTickers = {}
        for exchange in exchanges:
            initTickers[exchange] = self.get_tickers_from_mu_and_su_balances(initExchangeBalances[exchange])
            curTickers[exchange] = self.get_tickers_from_mu_and_su_balances(curExchangeBalances[exchange])

        #reformat exchange balances
        initExchangeBalancesReformat = {}
        curExchangeBalancesReformat = {}
        for exchange in exchanges:
            initExchangeBalancesReformat[exchange] = initExchangeBalances[exchange]['mu']
            curExchangeBalancesReformat[exchange] = curExchangeBalances[exchange]['mu']

        pnlInfo = self.get_pnl(initExchangeBalancesReformat, initBorrowBalances, initTickers,
                               curExchangeBalancesReformat, curBorrowBalances, curTickers, reserveCurrencyAdded)

        return pnlInfo

    def get_rebalancing_data(self, startTime, limit=100):
        sql = 'select * from "{}"."{}" where in_z > \'{}\' order by in_z asc limit {}'.format(self.botName, 'rebalance', startTime, limit)
        rebalanceDF = query(sql, environment=self.omsDataEnvironment, dataframe=True)
        rebalanceDF = rebalanceDF.set_index('in_z')
        rebalanceDF['result'] = rebalanceDF['result'].apply(json.loads)
        rebalanceDF = rebalanceDF.dropna()
        rebalanceDF = rebalanceDF[rebalanceDF['result'].apply(lambda x: 'result' in x and x['successOMS'])] #filter out sanity checks + fails

        if(len(rebalanceDF) > 0):
            rebalanceDF['result'] = rebalanceDF['result'].apply(lambda x: x['result'])
            #monkey patch bug
            def reformat_altsimPriceData(rebalanceData):
                if('binance' in rebalanceData['altSimPriceData']):
                    rebalanceData['altSimPriceData'] = rebalanceData['altSimPriceData']['binance']
                return rebalanceData
            rebalanceDF['result'] = rebalanceDF['result'].apply(reformat_altsimPriceData)

        return rebalanceDF

    def rebalance_round_to_exchange_balances(self, rebalanceRound):
        allExchangeBalances = {}
        for exchange in rebalanceRound:
            exchangeBalance = rebalanceRound[exchange]['result']['balanceHistory'][0]
            allExchangeBalances[exchange] = exchangeBalance['mu']
        return allExchangeBalances
    def rebalance_round_to_tickers(self, rebalanceRound):
        allExchangeTickers = {}
        for exchange in rebalanceRound:
            exchangeBalance = rebalanceRound[exchange]['result']['balanceHistory'][0]
            allExchangeTickers[exchange] = self.get_tickers_from_mu_and_su_balances(exchangeBalance)
        return allExchangeTickers
    def rebalance_round_to_rebalance_end_times(self, rebalanceRound):
        endTimes = {}
        for exchange in rebalanceRound:
            endTime = rebalanceRound[exchange]['result']['in_z']
            endTimes[exchange] = endTime
        return endTimes
    def rebalance_round_to_positions(self, rebalanceRound):
        allExchangePositions = {}
        for exchange in rebalanceRound:
            allExchangePositions[exchange] = rebalanceRound[exchange]['result']['actualEndSingleUnitLSBalance']
        return allExchangePositions
    def rebalance_round_to_orders(self, rebalanceRound):
        orders = []

        tickers = self.rebalance_round_to_tickers(rebalanceRound)

        for exchange in rebalanceRound:
            for order in rebalanceRound[exchange]['result']['rebalanceOrderInfo']:
                order['exchange'] = exchange
                order['omsType']  = 'rebalance'
                order['reserveCurrencyCost'] = order['filled']*tickers[exchange][order['symbol'].split('/')[0]]
                orders.append(order)
            for order in rebalanceRound[exchange]['result']['bnbAdjustmentOrderInfo']:
                order['exchange'] = exchange
                order['omsType'] = 'cushion'
                order['reserveCurrencyCost'] = order['filled'] * tickers[exchange][order['symbol'].split('/')[0]]
                orders.append(order)
            for order in rebalanceRound[exchange]['result']['cushionAdjustmentOrderInfo']:
                order['exchange'] = exchange
                order['omsType'] = 'cushion'
                order['reserveCurrencyCost'] = order['filled'] * tickers[exchange][order['symbol'].split('/')[0]]
                orders.append(order)
        return orders

    def get_borrow_balances_after_times(self, times, borrowBalanceHistoryAllExchanges):
        borrowBalanceAllExchanges = {}
        for exchange in borrowBalanceHistoryAllExchanges:
            borrowBalanceHistory = borrowBalanceHistoryAllExchanges[exchange]
            borrowBalance = borrowBalanceHistory[borrowBalanceHistory.index <= times[exchange]].iloc[-1]
            borrowBalanceAllExchanges[exchange] = borrowBalance
        return borrowBalanceAllExchanges
    def get_reserve_currency_added_after_times(self, times, rcAddedHistoryAllExchanges):
        rcAddedAllExchanges = {}
        for exchange in rcAddedHistoryAllExchanges:
            if(len(rcAddedHistoryAllExchanges[exchange])>0):
                rcAddedHistory = rcAddedHistoryAllExchanges[exchange]
                rcAddedAllExchanges[exchange] = rcAddedHistory[rcAddedHistory.index <= times[exchange]]['reserveCurrencyAdded'].sum()
            else:
                rcAddedAllExchanges[exchange] = 0
        return rcAddedAllExchanges

    def get_partial_pnl(self, initRebalancingRound, rebalanceDataHistoryAllExchanges, borrowBalanceHistoryAllExchanges,
                        rcAddedHistoryAllExchanges):
        exchanges = rebalanceDataHistoryAllExchanges['exchange'].unique()

        initExchangeBalances= self.rebalance_round_to_exchange_balances(initRebalancingRound)
        initTickers = self.rebalance_round_to_tickers(initRebalancingRound)
        initRebalanceTimes = self.rebalance_round_to_rebalance_end_times(initRebalancingRound)
        initBorrowBalances = self.get_borrow_balances_after_times(initRebalanceTimes, borrowBalanceHistoryAllExchanges)
        initRCAdded = self.get_reserve_currency_added_after_times(initRebalanceTimes, rcAddedHistoryAllExchanges)

        pnlHistory = []

        curRebalancingRound = {}
        for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
            row['result']['in_z'] = idx
            curRebalancingRound[row['exchange']] = row
            if(len(curRebalancingRound) == len(exchanges)):
                curExchangeBalances = self.rebalance_round_to_exchange_balances(curRebalancingRound)
                curTickers = self.rebalance_round_to_tickers(curRebalancingRound)
                curRebalanceTimes = self.rebalance_round_to_rebalance_end_times(curRebalancingRound)
                curBorrowBalances = self.get_borrow_balances_after_times(curRebalanceTimes,
                                                                          borrowBalanceHistoryAllExchanges)
                curRCAdded = self.get_reserve_currency_added_after_times(curRebalanceTimes,
                                                                             rcAddedHistoryAllExchanges)
                netRCAdded = {}
                for exchange in exchanges:
                    netRCAdded[exchange] = curRCAdded[exchange]-initRCAdded[exchange]
                pnlInfo = self.get_pnl(initExchangeBalances, initBorrowBalances, initTickers,
                             curExchangeBalances, curBorrowBalances, curTickers, netRCAdded)
                pnlInfo['time'] = max([curRebalanceTimes[exchange] for exchange in curRebalanceTimes])
                pnlHistory.append(pnlInfo)
                curRebalancingRound = {}

        return pnlHistory

    def get_partial_positions(self, rebalanceDataHistoryAllExchanges):
        exchanges = rebalanceDataHistoryAllExchanges['exchange'].unique()

        positionHistory = []

        curRebalancingRound = {}
        for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
            row['result']['in_z'] = idx
            curRebalancingRound[row['exchange']] = row
            if (len(curRebalancingRound) == len(exchanges)):
                curPositions = self.rebalance_round_to_positions(curRebalancingRound)
                curTimes = self.rebalance_round_to_rebalance_end_times(curRebalancingRound)
                curPositions['time'] = max([curTimes[exchange] for exchange in curTimes])
                positionHistory.append(curPositions)
                curRebalancingRound = {}
        return positionHistory

    def get_partial_turnover(self, rebalanceDataHistoryAllExchanges):
        exchanges = rebalanceDataHistoryAllExchanges['exchange'].unique()

        turnoverHistory = []

        curRebalancingRound = {}
        for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
            row['result']['in_z'] = idx
            curRebalancingRound[row['exchange']] = row
            if (len(curRebalancingRound) == len(exchanges)):
                allOrders = self.rebalance_round_to_orders(curRebalancingRound)
                curRebalanceTimes = self.rebalance_round_to_rebalance_end_times(curRebalancingRound)
                turnoverInfo = {'total':0,'byExchange':defaultdict(float),'byMarket':{},'cushion':0}
                for exchange in exchanges:
                    turnoverInfo['byMarket'][exchange] = defaultdict(float)
                for order in allOrders:
                    turnoverInfo['total'] += order['reserveCurrencyCost']
                    turnoverInfo['byExchange'][order['exchange']] += order['reserveCurrencyCost']
                    turnoverInfo['byMarket'][order['exchange']][order['symbol'].split('/')[0]] += order['reserveCurrencyCost']
                    if(order['omsType'] == 'cushion'):
                        turnoverInfo['cushion'] = order['reserveCurrencyCost']
                turnoverInfo['time'] = max([curRebalanceTimes[exchange] for exchange in curRebalanceTimes])
                turnoverHistory.append(turnoverInfo)
                curRebalancingRound = {}
        return turnoverHistory

    def get_partial_exchange_balances(self, rebalanceDataHistoryAllExchanges):
        exchanges = rebalanceDataHistoryAllExchanges['exchange'].unique()

        exchangeBalanceHistory = []

        curRebalancingRound = {}
        for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
            row['result']['in_z'] = idx
            curRebalancingRound[row['exchange']] = row
            if (len(curRebalancingRound) == len(exchanges)):
                curRebalanceTimes = self.rebalance_round_to_rebalance_end_times(curRebalancingRound)
                exchangeBalance = self.rebalance_round_to_exchange_balances(curRebalancingRound)
                exchangeBalance['time'] = max([curRebalanceTimes[exchange] for exchange in curRebalanceTimes])
                exchangeBalanceHistory.append(exchangeBalance)
                curRebalancingRound = {}

        return exchangeBalanceHistory

    def get_partial_slippage(self, rebalanceDataHistoryAllExchanges, slippageInfo):
        def cc_rc_conversion(order, curRebalancingRound, tickers):
            altsimPrices = curRebalancingRound[order['exchange']]['result']['altSimPriceData']
            rc = self.reserveCurrencies[order['exchange']]

            b = order['symbol'].split('/')[0]
            q = altsimPrices.keys()[0].split('/')[1]
            if('{}/{}'.format(b, q) not in altsimPrices or
                    math.isnan(altsimPrices['{}/{}'.format(b, q)])):
                return None,None,None

            altsimPrice = altsimPrices['{}/{}'.format(b, q)]

            q = order['symbol'].split('/')[1]
            cost = order['cost']
            idealPrice = (order['ticker']['ask'] * .5 + order['ticker']['bid'] * .5)
            if(q != rc):
                cost *= tickers[order['exchange']][q]
                idealPrice *= tickers[order['exchange']][q]

            return altsimPrice, cost, idealPrice

        exchanges = rebalanceDataHistoryAllExchanges['exchange'].unique()

        curRebalancingRound ={}
        for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
            row['result']['in_z'] = idx
            curRebalancingRound[row['exchange']] = row
            if (len(curRebalancingRound) == len(exchanges)):
                orders = self.rebalance_round_to_orders(curRebalancingRound)
                tickers = self.rebalance_round_to_tickers(curRebalancingRound)
                for order in orders:
                    if(order['filled'] != 0):
                        altsimPrice, cost, idealPrice = cc_rc_conversion(order, curRebalancingRound, tickers)

                        if(altsimPrice is None):
                            continue

                        if (order['side'] == 'buy'):
                            actualSlippage = (altsimPrice - cost/order['filled'])*order['filled']
                            idealSlippage = (altsimPrice - idealPrice)*order['filled']
                        else:
                            actualSlippage = (cost/order['filled'] - altsimPrice) * order['filled']
                            idealSlippage = (-altsimPrice + idealPrice) * order['filled']

                        slippageInfo['total']['actualSlippage']+=actualSlippage
                        slippageInfo['total']['idealSlippage']+=idealSlippage
                        slippageInfo['total']['cost']+=cost
                        slippageInfo['byExchange'][order['exchange']]['actualSlippage']+=actualSlippage
                        slippageInfo['byExchange'][order['exchange']]['idealSlippage']+=idealSlippage
                        slippageInfo['byExchange'][order['exchange']]['cost']+=cost
                        slippageInfo['byMarket'][order['exchange']][order['symbol']]['actualSlippage'] += actualSlippage
                        slippageInfo['byMarket'][order['exchange']][order['symbol']]['idealSlippage'] += idealSlippage
                        slippageInfo['byMarket'][order['exchange']][order['symbol']]['cost'] += cost
                curRebalancingRound = {}

    def get_partial_win_percent(self, rebalanceDataHistoryAllExchanges, borrowBalanceHistoryAllExchanges, rcAddedHistoryAllExchanges, winPercentInfo):
        exchanges = rebalanceDataHistoryAllExchanges['exchange'].unique()

        def get_delta(stats0,stats1):
            delta = {}
            for exchange in stats0:
                delta[exchange] = {}
                for coin in stats0[exchange]:
                    delta[exchange][coin] = stats1[exchange][coin] - stats0[exchange][coin]
            return delta

        curRebalancingRound = {}
        lastRebalancingRound = None
        for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
            row['result']['in_z'] = idx
            curRebalancingRound[row['exchange']] = row
            if (len(curRebalancingRound) == len(exchanges)):
                if(lastRebalancingRound is None):
                    lastRebalancingRound = curRebalancingRound
                else:
                    #get price deltas
                    deltaPrice = get_delta(self.rebalance_round_to_tickers(lastRebalancingRound),
                                           self.rebalance_round_to_tickers(curRebalancingRound))
                    lastPositions = self.rebalance_round_to_positions(lastRebalancingRound)
                    curPositions = self.rebalance_round_to_positions(curRebalancingRound)
                    deltaPositions = get_delta(lastPositions, curPositions)

                    for exchange in lastPositions:
                        for coin in lastPositions[exchange]:
                            if(np.sign(deltaPrice[exchange][coin]) == np.sign(curPositions[exchange][coin])):
                                winPercentInfo['coinPositionWinPct'][exchange][coin]['wins']+=1
                            winPercentInfo['coinPositionWinPct'][exchange][coin]['total']+=1
                            if(np.sign(deltaPrice[exchange][coin]) == np.sign(deltaPositions[exchange][coin])):
                                winPercentInfo['coinPositionDeltaWinPct'][exchange][coin]['wins'] += 1
                            winPercentInfo['coinPositionDeltaWinPct'][exchange][coin]['total'] += 1

                    lastExchangeBalances = self.rebalance_round_to_exchange_balances(lastRebalancingRound)
                    curExchangeBalances = self.rebalance_round_to_exchange_balances(curRebalancingRound)
                    lastTickers = self.rebalance_round_to_tickers(lastRebalancingRound)
                    curTickers = self.rebalance_round_to_tickers(curRebalancingRound)
                    lastRebalanceTimes = self.rebalance_round_to_rebalance_end_times(lastRebalancingRound)
                    lastBorrowBalances = self.get_borrow_balances_after_times(lastRebalanceTimes,
                                                                             borrowBalanceHistoryAllExchanges)
                    curRebalanceTimes = self.rebalance_round_to_rebalance_end_times(curRebalancingRound)
                    curBorrowBalances = self.get_borrow_balances_after_times(curRebalanceTimes,
                                                                              borrowBalanceHistoryAllExchanges)
                    lastRCAdded = self.get_reserve_currency_added_after_times(curRebalanceTimes,
                                                                             rcAddedHistoryAllExchanges)
                    curRCAdded = self.get_reserve_currency_added_after_times(curRebalanceTimes,
                                                                             rcAddedHistoryAllExchanges)
                    netRCAdded = {}
                    for exchange in exchanges:
                        netRCAdded[exchange] = curRCAdded[exchange] - lastRCAdded[exchange]
                    pnlInfo = self.get_pnl(lastExchangeBalances, lastBorrowBalances, lastTickers,
                                           curExchangeBalances, curBorrowBalances, curTickers, netRCAdded)
                    if(np.sign(pnlInfo['combined']) == 1):
                        winPercentInfo['overall']['wins'] += 1

                    winPercentInfo['overall']['total'] += 1

                    lastRebalancingRound = curRebalancingRound
                curRebalancingRound = {}

    def get_bot_stats(self, startTime, endTime,
                      getPnl=True,
                      getTurnover=True,
                      getBalances=True,
                      getPositions=True,
                      getSlippage=True,
                      getWinPercent=True):
        assert startTime < endTime

        exchanges = self.get_exchanges_traded_on()

        #pnl variables
        borrowBalanceHistoryAllExchanges = None
        rcAddedAllexchanges = None
        initRebalancingRound = None
        allPnlInfo = []

        # turnover variables
        allTurnoverInfo = []

        # exchange balance history
        allExchangeBalances = []

        #positions variables
        allPositions = []

        #slippage variables
        slippageInfo = {'total': {'idealSlippage':0, 'actualSlippage':0, 'cost':0},
                        'byExchange': defaultdict(lambda:defaultdict(float)),
                        'byMarket': defaultdict(lambda:defaultdict(lambda:defaultdict(float)))}

        #win percent info
        winPercentInfo = {'coinPositionWinPct':defaultdict(lambda:defaultdict(lambda:defaultdict(float))),
                          'coinPositionDeltaWinPct':defaultdict(lambda:defaultdict(lambda:defaultdict(float))),
                          'overall':{'wins':0.0,'total':0.0}}

        if(getPnl or getWinPercent):
            # get borrow balance history
            borrowBalanceHistoryAllExchanges = {}
            rcAddedAllexchanges = {}
            for exchange in exchanges:
                borrowBalanceHistoryAllExchanges[exchange] = self.get_borrow_balances(exchange)
                rcAddedAllexchanges[exchange] = self.get_reserve_currency_added(exchange)
            assert len(borrowBalanceHistoryAllExchanges) > 0

        rebalanceRoundPullsPerQuery = 1000

        moreData = True
        firstRound = True
        while moreData:
            rebalanceDataHistoryAllExchanges = self.get_rebalancing_data(startTime, limit=rebalanceRoundPullsPerQuery)
            if(len(rebalanceDataHistoryAllExchanges) == 0):
                break
            if(rebalanceDataHistoryAllExchanges.index.max() >= endTime):
                moreData = False
                rebalanceDataHistoryAllExchanges = rebalanceDataHistoryAllExchanges[rebalanceDataHistoryAllExchanges.index <= endTime]

            if(getPnl):
                if(firstRound):
                    #get initRebalanceRound
                    initRebalancingRound = {}
                    for idx, row in rebalanceDataHistoryAllExchanges.iterrows():
                        row['result']['in_z'] = idx
                        initRebalancingRound[row['exchange']] = row
                        if (len(initRebalancingRound) == len(exchanges)):
                            break
                    assert len(initRebalancingRound) == len(exchanges)

                partialPnlInfo = self.get_partial_pnl(initRebalancingRound, rebalanceDataHistoryAllExchanges,
                                        borrowBalanceHistoryAllExchanges, rcAddedAllexchanges)
                allPnlInfo += partialPnlInfo

            if(getTurnover):
                allTurnoverInfo += self.get_partial_turnover(rebalanceDataHistoryAllExchanges)

            if(getBalances):
                allExchangeBalances += self.get_partial_exchange_balances(rebalanceDataHistoryAllExchanges)

            if(getPositions):
                allPositions += self.get_partial_positions(rebalanceDataHistoryAllExchanges)

            if(getSlippage):
                self.get_partial_slippage(rebalanceDataHistoryAllExchanges, slippageInfo)

            if(getWinPercent):
                self.get_partial_win_percent(rebalanceDataHistoryAllExchanges, borrowBalanceHistoryAllExchanges,
                                             rcAddedAllexchanges, winPercentInfo)

            startTime = rebalanceDataHistoryAllExchanges.index.max()
            firstRound = False

        # get slippage percents
        if(getSlippage):
            def get_percent_slippage(slippageDict):
                slippageDict['actualSlippagePercent'] = slippageDict['actualSlippage'] / slippageDict['cost']
                slippageDict['idealSlippagePercent'] = slippageDict['idealSlippage'] / slippageDict['cost']
            get_percent_slippage(slippageInfo['total'])
            for exchange in slippageInfo['byExchange']:
                get_percent_slippage(slippageInfo['byExchange'][exchange])
            for exchange in slippageInfo['byMarket']:
                for market in slippageInfo['byMarket'][exchange]:
                    get_percent_slippage(slippageInfo['byMarket'][exchange][market])

        #get win percents
        if(getWinPercent):
            for exchange in winPercentInfo['coinPositionWinPct']:
                for coin in winPercentInfo['coinPositionWinPct'][exchange]:
                    winDict = winPercentInfo['coinPositionWinPct'][exchange][coin]
                    winPercentInfo['coinPositionWinPct'][exchange][coin]['pct'] = winDict['wins']/winDict['total']
            for exchange in winPercentInfo['coinPositionDeltaWinPct']:
                for coin in winPercentInfo['coinPositionDeltaWinPct'][exchange]:
                    winDict = winPercentInfo['coinPositionDeltaWinPct'][exchange][coin]
                    winPercentInfo['coinPositionDeltaWinPct'][exchange][coin]['pct'] = winDict['wins']/winDict['total']
            winPercentInfo['overall']['pct'] = winPercentInfo['overall']['wins']/winPercentInfo['overall']['total']

        return allPnlInfo, allPositions, allTurnoverInfo, allExchangeBalances, slippageInfo, winPercentInfo

    def stats_to_csv(self, startTime, endTime,
                      getPnl=True,
                      getTurnover=True,
                      getBalances=True,
                      getPositions=True,
                      getSlippage=True,
                      getWinPercent=True):

        pnlInfo, allPositions, turnoverInfo, allExchangeBalances, slippageInfo, winPercentInfo = self.get_bot_stats(
            startTime, endTime,
            getPnl,
            getTurnover,
            getBalances,
            getPositions,
            getSlippage,
            getWinPercent)

        def json_serial(obj):
            if hasattr(obj, 'to_json'):
                return obj.to_json()
            elif (type(obj) == pd.Timestamp):
                return str(obj)

            return json.JSONEncoder.default(self, obj)

        if (not os.path.isdir(self.botstatsFolder)):
            os.mkdir(os.path.join(self.botstatsFolder))

        jsonFilesDir = os.path.join(self.botstatsFolder, 'stats_json_files')
        if(not os.path.isdir(jsonFilesDir)):
            os.mkdir(jsonFilesDir)
        else:
            shutil.rmtree(jsonFilesDir)
            os.mkdir(jsonFilesDir)

        if(getPnl):
            with open('{}/pnl.json'.format(jsonFilesDir), 'w') as f:
                json.dump(pnlInfo, f, default=json_serial)
        if (getTurnover):
            with open('{}/turnover.json'.format(jsonFilesDir), 'w') as f:
                json.dump(turnoverInfo, f, default=json_serial)
        if (getBalances):
            with open('{}/balances.json'.format(jsonFilesDir), 'w') as f:
                json.dump(allExchangeBalances, f, default=json_serial)
        if (getPositions):
            with open('{}/positions.json'.format(jsonFilesDir), 'w') as f:
                json.dump(allPositions, f, default=json_serial)
        if (getSlippage):
            with open('{}/slippage.json'.format(jsonFilesDir), 'w') as f:
                json.dump(slippageInfo, f, default=json_serial)
        if (getWinPercent):
            with open('{}/winPct.json'.format(jsonFilesDir), 'w') as f:
                json.dump(winPercentInfo, f, default=json_serial)
        print('Done')

    def visualize_stats(self, startTime, endTime,
                      getPnl=True,
                      getTurnover=True,
                      getBalances=True,
                      getPositions=True,
                      getSlippage=True,
                      getWinPercent=True):

        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.pylab import savefig

        if(not os.path.isdir(self.botstatsFolder)):
            os.mkdir(os.path.join(self.botstatsFolder))
        for statsType in ['slippage_by_market', 'positions_by_coin', 'turnover_by_coin']:
            if(not os.path.isdir(os.path.join(self.botstatsFolder, statsType))):
                os.mkdir(os.path.join(self.botstatsFolder, statsType))

        pnlInfo, allPositions, turnoverInfo, allExchangeBalances, slippageInfo, winPercentInfo = self.get_bot_stats(startTime, endTime,
                      getPnl,
                      getTurnover,
                      getBalances,
                      getPositions,
                      getSlippage,
                      getWinPercent)

        e2b = {'binance': 'BINA', 'kraken': 'KRKN'}

        if(getSlippage):
            def view_slippage(slippageType, slippageDict):
                print('{} Actual:{}, Ideal:{}'.format(slippageType, slippageDict['actualSlippagePercent'],
                                                      slippageDict['idealSlippagePercent']))

            def plot_slippage(slippageIdealPct, slippageActualPct, path, label):
                slippageIdealPct*=100
                slippageActualPct*=100
                fig, ax = plt.subplots()
                n_groups = 1
                index = np.arange(n_groups)
                bar_width = .35
                rects1 = ax.bar(index, [slippageIdealPct], bar_width, label='Ideal')
                rects2 = ax.bar(index + bar_width, [slippageActualPct], bar_width, label='Actual')
                ax.set_ylabel('Slippage %')
                ax.set_title(label + ' Slippage')
                ax.set_xticks(index + bar_width / 2)
                ax.set_xticklabels((''))
                ax.legend()
                fig.tight_layout()
                savefig(path, bbox_inches='tight')
                plt.clf()

            plot_slippage(slippageInfo['total']['idealSlippagePercent'],
                          slippageInfo['total']['actualSlippagePercent'],
                          os.path.join(self.botstatsFolder, 'total_slippage.png'), 'Total')
            # view_slippage('Total', slippageInfo['total'])
            for exchange in slippageInfo['byExchange']:
                # view_slippage('{} Total'.format(exchange), slippageInfo['byExchange'][exchange])
                plot_slippage(slippageInfo['byExchange'][exchange]['idealSlippagePercent'],
                              slippageInfo['byExchange'][exchange]['actualSlippagePercent'],
                              os.path.join(self.botstatsFolder, '{}_slippage.png'.format(exchange)), e2b[exchange])
            for exchange in slippageInfo['byMarket']:
                for market in slippageInfo['byMarket'][exchange]:
                    mktLabel = '_'.join(market.split('/'))
                    plot_slippage(slippageInfo['byMarket'][exchange][market]['idealSlippagePercent'],
                                  slippageInfo['byMarket'][exchange][market]['actualSlippagePercent'],
                                  os.path.join(self.botstatsFolder, 'slippage_by_market',
                                               '{}_{}_slippage.png'.format(exchange, mktLabel))
                                  , '_'.join([e2b[exchange], mktLabel]))
                    # view_slippage('Total {}, {}'.format(exchange, market), slippageInfo['byMarket'][exchange][market])

        if(getPnl):
            combinedPNL = [info['combined'] for info in pnlInfo]
            times = [info['time'] for info in pnlInfo]
            ax = pd.Series(data=combinedPNL, index=times).plot()
            ax.set_title('Total PnL')
            ax.set_ylabel('USD')
            savefig(os.path.join(self.botstatsFolder,'total_pnl.png'), bbox_inches='tight')

            for exchange in pnlInfo[0]['net']:
                pnlExchange = [pnl['net'][exchange] for pnl in pnlInfo]
                ax = pd.Series(data=pnlExchange, index=times).plot()
                ax.set_title('{} PnL'.format(e2b[exchange]))
                ax.set_ylabel('USD')
                savefig(os.path.join(self.botstatsFolder, '{}_pnl.png'.format(exchange)), bbox_inches='tight')

        if(getPositions):
            exchangeToPosHistory = defaultdict(list)
            for positions in allPositions:
                for exchange in positions:
                    if exchange != 'time':
                        exchangeToPosHistory[exchange].append(positions[exchange])
            times = [positions['time'] for positions in allPositions]
            for exchange in exchangeToPosHistory:
                posDF = pd.DataFrame(data=exchangeToPosHistory[exchange], index=times)
                ax = posDF.plot()
                ax.set_ylabel('USD')
                ax.set_title('{} Long/Short Positions'.format(e2b[exchange]))
                savefig(os.path.join(self.botstatsFolder,'{}_positions.png'.format(exchange)), bbox_inches='tight')
                plt.clf()
                for coin in posDF:
                    ax = posDF[coin].plot(label=coin + '(' + exchange + ')')
                    plt.legend()
                    ax.set_ylabel('USD')
                    ax.set_title('{}_{} Long/Short Positions'.format(e2b[exchange], coin))
                    savefig(os.path.join(self.botstatsFolder,'positions_by_coin', '{}_{}_positions.png'.format(exchange, coin)),
                            bbox_inches='tight')
                    plt.clf()

        if(getTurnover):
            times = [positions['time'] for positions in allPositions]
            totalHistory = [turnover['total'] for turnover in turnoverInfo]
            ax = pd.Series(data=totalHistory, index=times).plot()
            ax.set_ylabel('USD')
            ax.set_title('Total Turnover Per Rebalance Round')
            savefig(os.path.join(self.botstatsFolder, 'total_turnover.png'), bbox_inches='tight')
            plt.clf()
            ax = pd.Series(data=totalHistory, index=times).resample('1D').apply(sum).plot()
            ax.set_ylabel('USD')
            ax.set_title('Total Daily Turnover')
            savefig(os.path.join(self.botstatsFolder, 'total_turnover_daily.png'), bbox_inches='tight')
            plt.clf()
            cushionHistory = [turnover['cushion'] for turnover in turnoverInfo]
            ax = pd.Series(data=cushionHistory, index=times).plot()
            ax.set_ylabel('USD')
            ax.set_title('Total Cushion Turnover Per Rebalance Round')
            savefig(os.path.join(self.botstatsFolder,'total_cushion_turnover.png'), bbox_inches='tight')
            plt.clf()
            ax = pd.Series(data=cushionHistory, index=times).resample('1D').apply(sum).plot()
            ax.set_ylabel('USD')
            ax.set_title('Total Daily Cushion Turnover')
            savefig(os.path.join(self.botstatsFolder, 'total_cushion_turnover_daily.png'), bbox_inches='tight')
            plt.clf()
            totalExchangeHistory = [turnover['byExchange'] for turnover in turnoverInfo]
            totalExchangeHistory = pd.DataFrame(data=totalExchangeHistory, index=times)
            for exchange in totalExchangeHistory:
                ax = totalExchangeHistory[exchange].plot()
                ax.set_ylabel('USD')
                ax.set_title('{} Total Turnover Per Rebalance Round'.format(e2b[exchange]))
                savefig(os.path.join(self.botstatsFolder, '{}_turnover.png'.format(exchange)), bbox_inches='tight')
                plt.clf()
            totalMarketHistory = defaultdict(list)
            for turnover in turnoverInfo:
                for exchange in turnover['byMarket']:
                    totalMarketHistory[exchange].append(turnover['byMarket'][exchange])

            totalTurnoverEachMarket = []
            for exchange in totalMarketHistory:
                totalExchangeMarketHistory = pd.DataFrame(data=totalMarketHistory[exchange], index=times)
                for market in totalExchangeMarketHistory:
                    totalExchangeMarketHistory[market] = totalExchangeMarketHistory[market].fillna(0)
                    totalTurnoverEachMarket.append((market, totalExchangeMarketHistory[market].sum()))
                    ax = totalExchangeMarketHistory[market].plot()
                    ax.set_ylabel('USD')
                    ax.set_title('{}_{} Total Turnover Per Rebalance Round'.format(e2b[exchange], market))
                    savefig(os.path.join(self.botstatsFolder, 'turnover_by_coin', '{}_{}_turnover.png'.format(exchange, market)),
                            bbox_inches='tight')
                    plt.clf()

        print('Position Win Pct')
        for exchange in winPercentInfo['coinPositionWinPct']:
            for coin in winPercentInfo['coinPositionWinPct'][exchange]:
                winDict = winPercentInfo['coinPositionWinPct'][exchange][coin]
                print('Exchange:{}, Coin:{}, Wins:{}, Total:{}, Pct:{}'.format(exchange, coin, winDict['wins'],
                                                                               winDict['total'], winDict['pct']))
        print('Position Delta Win Pct')
        for exchange in winPercentInfo['coinPositionDeltaWinPct']:
            for coin in winPercentInfo['coinPositionDeltaWinPct'][exchange]:
                winDict = winPercentInfo['coinPositionDeltaWinPct'][exchange][coin]
                print('Exchange:{}, Coin:{}, Wins:{}, Total:{}, Pct:{}'.format(exchange, coin, winDict['wins'],
                                                                               winDict['total'], winDict['pct']))
        print('Overall')
        winPercentInfo['overall']['pct'] = winPercentInfo['overall']['wins'] / winPercentInfo['overall']['total']
        print('Wins:{}, Total:{}, Pct:{}'.format(winPercentInfo['overall']['wins'], winPercentInfo['overall']['total'],
                                                 winPercentInfo['overall']['pct']))

        print('Done')

#omsDataAnalyzer = OMSDataAnalytics('jaekimopt005')
#omsDataAnalyzer.stats_to_csv(pd.Timestamp('2018-08-16 01:13:00'), pd.Timestamp('2018-09-01 20:25:35.067602'))

#pnlInfo, allPositions, turnoverInfo, allExchangeBalances, slippageInfo, winPercentInfo = omsDataAnalyzer.get_bot_stats(
#                    pd.Timestamp('2018-08-16 01:13:00'), pd.Timestamp('2018-09-01 20:25:35.067602'))
# pnlAll = []
# for pnl in pnlInfo:
#     print(pnl['time'], pnl['combined'])
    # totalPnl = {}
    # totalPnl['time'] = pnl['time']
    # totalPnl['pnl'] = pnl['combined']
    # pnlAll.append(totalPnl)
# pnlAllDf = pd.DataFrame(pnlAll)
# pnlAllDf.to_csv('pnl_os.csv')


# print(pnlInfo)
# print('\n')
# print(allPositions)
# print('\n')
# print(turnoverInfo)
# print('\n')
# print(allExchangeBalances)
# print('\n')
# print(slippageInfo)
# print('\n')
# print(winPercentInfo)
# print('\n')
# print(omsDataAnalyzer.get_current_pnl(pd.Timestamp('2018-08-16 01:13:00')))
#omsDataAnalyzer.visualize_stats(pd.Timestamp('2018-08-16 01:13:00'), pd.Timestamp('2018-09-01 20:20:35.067602'))

# def plot_slippage2():
#     fig, ax = plt.subplots()
#     n_groups = 2
#     index = np.arange(n_groups)
#     bar_width = .35
#     rects1 = ax.bar(index, [.22, .238], bar_width, label='Buy Savings')
#     rects2 = ax.bar(index + bar_width, [.145, .115], bar_width, label='Sell Savings')
#     ax.set_ylabel('% Savings')
#     ax.set_title('OMS Slippage Savings vs Market Order')
#     ax.set_xticks(index + bar_width / 2)
#     ax.set_xticklabels(('KRKN_ETC_USD', 'BINA_OMG_BTC'))
#     ax.legend()
#     fig.tight_layout()
#     savefig('analytics/botstats/slippage_oms_vs_market.png', bbox_inches='tight')
# plot_slippage2()
# print('done')
# input()

#omsDataAnalyzer = OMSDataAnalytics('jaekimopt003')
#pnlInfo, allPositions, turnoverInfo, allExchangeBalances, slippageInfo = omsDataAnalyzer.get_bot_stats(pd.Timestamp('2018-05-17 00:00:00'), pd.Timestamp('2018-05-23 00:00:00'))
# def plot_slippage(slippageIdealPct, slippageActualPct):
#     fig, ax = plt.subplots()
#     n_groups = len(slippageActualPct)
#     index = np.arange(n_groups)
#     bar_width = .35
#     rects1 = ax.bar(index, slippageIdealPct, bar_width, label='Ideal')
#     rects2 = ax.bar(index+bar_width, slippageActualPct, bar_width, label='Actual')
#     ax.set_ylabel('Slippage %')
#     ax.set_title('Slippage Stats')
#     ax.set_xticks(index + bar_width / 2)
#     ax.set_xticklabels(('Total','KRKN','BINA','BINA_BAT_BTC'))
#     ax.legend()
#     fig.tight_layout()
#     savefig('analytics/botstats/slippage_info.png', bbox_inches='tight')
#
#
# slippageIdealPct=[]
# slippageIdealPct.append(slippageInfo['total']['idealSlippagePercent']*100)
# slippageIdealPct.append(slippageInfo['byExchange']['kraken']['idealSlippagePercent']*100)
# slippageIdealPct.append(slippageInfo['byExchange']['binance']['idealSlippagePercent']*100)
# slippageIdealPct.append(slippageInfo['byMarket']['binance']['BAT/BTC']['idealSlippagePercent']*100)
# slippageActualPct=[]
# slippageActualPct.append(slippageInfo['total']['actualSlippagePercent']*100)
# slippageActualPct.append(slippageInfo['byExchange']['kraken']['actualSlippagePercent']*100)
# slippageActualPct.append(slippageInfo['byExchange']['binance']['actualSlippagePercent']*100)
# slippageActualPct.append(slippageInfo['byMarket']['binance']['BAT/BTC']['actualSlippagePercent']*100)
# plot_slippage(slippageIdealPct, slippageActualPct)