import sys
from Exchanges.Base.Exchange import Exchange
from ccxt.kraken import kraken
import time
from utils.funcs import run_function_safe
import pandas as pd
from Exchanges.Base.OrderBook import OrderBook

class Kraken(Exchange):
    def __init__(self, keypath):
        with open(keypath, "r") as f:
            key = f.readline().strip()
            secret = f.readline().strip()
        self.useCoinapi = True
        self.ledgerCodeToCommonCode = {'XXRP': 'XRP', 'BCH': 'BCH', 'XXBT': 'BTC', 'XETC': 'ETC', 'XETH': 'ETH',
                                       'ZUSD': 'USD', 'XZEC': 'ZEC', 'XXMR': 'XMR', 'XXLM': 'XLM'}
        self.quoteOnly = set(['USD'])
        Exchange.__init__(self, 'Kraken', kraken({'apiKey': key, 'secret': secret}))
        self.orderPlacementFreq = 1

    def get_balances(self, balanceDict = None):
        coins = run_function_safe(self.api.fetchCurrencies).keys()
        balances = run_function_safe(self.api.fetch_balance)
        for coin in coins:
            if coin not in balances:
                balances[coin] = {'total': 0, 'used': 0, 'free': 0}
                balances['total'][coin] = 0
                balances['used'][coin] = 0
                balances['free'][coin] = 0

        if(balanceDict is None):
            return balances
        else:
            balanceDict[self.exchange_name.lower()] = balances


    def limit_buy(self, market, amount, highest_rate, *args):
        return run_function_safe(self.api.create_limit_buy_order, market, amount, highest_rate, *args)

    def market_buy(self, market, amount, req=None):
        return run_function_safe(self.api.create_market_buy_order, market, amount)

    def limit_sell(self, market, amount, lowest_rate, *args):
        return run_function_safe(self.api.create_limit_sell_order, market, amount, lowest_rate, *args)

    def market_sell(self, market, amount, req=None):
        return run_function_safe(self.api.create_market_sell_order, market, amount)

    def choose_route(self, routes):
        #choose shortest route and avoid ETH markets if possible
        allRouteLens = list(map(lambda x: len(x), routes))
        minRouteLength = min(allRouteLens)
        hasETH = list(map(lambda x: self.routeFinder.uses_quote_market(x, 'ETH'), routes))

        for i, route in enumerate(routes):
            if(len(route)==minRouteLength and not(hasETH[i])):
                return route

        return routes[allRouteLens.index(minRouteLength)]

    def cancel_order(self, order_id, symbol=None):
        return run_function_safe(self.api.cancel_order, order_id)

    # todo integrate symbols
    def fetch_open_orders(self, symbols=None):
        return run_function_safe(self.api.fetch_open_orders)

    # todo integrate symbols
    def fetch_closed_orders(self, symbols=None):
        return run_function_safe(self.api.fetchClosedOrders)

    def cancel_open_orders(self, symbols=None):
        cancelOrderInfo = []
        openOrders = self.fetch_open_orders()
        for orderInfo in openOrders:
            if (symbols is None or orderInfo['symbol'] in symbols):
                cancelOrderInfo.append(self.cancel_order(self.get_order_id(orderInfo)))
        return {'result':cancelOrderInfo}

    def get_ob(self, market, depth=100):
        obUnformatted = run_function_safe(self.api.fetch_order_book, market)        
        return OrderBook(obUnformatted['bids'], obUnformatted['asks']) 

    def get_order_stats(self, order_id, timestamp=None, marketSym=None):
        return run_function_safe(self.api.fetch_order, order_id)

    def get_order_id(self, orderInfo):
        return orderInfo['id']

    def update_fees(self):
        feeInfo = run_function_safe(self.api.fetch_trading_fees)

        for market in run_function_safe(self.api.fetch_markets):
            pair = market['symbol']
            self.fees[pair] = Fees(maker=feeInfo['maker'], taker=feeInfo['taker'])

    def get_fees(self, _):
        feeInfo = run_function_safe(self.api.fetch_trading_fees)
        return Fees(maker=feeInfo['maker'], taker=feeInfo['taker'])

    def get_ledger_items(self, startTime, endTime, itemType, inclusive=False):
        allItems = pd.DataFrame()
        moreItems = True

        # convert datetime to unix timestamp (seconds)
        startTime = startTime.timestamp()
        endTime = endTime.timestamp()

        while moreItems:
            endTime = allItems['time'].min() - 1 if len(allItems) else endTime

            self.log_it('info', "Loaded items since: {}".format(endTime))

            # get ledger data + reformat to dataframe
            rawItems = run_function_safe(self.api.privatePostLedgers, {'type': itemType, 'end': endTime})
            reformattedItems = []
            for key, value in rawItems['result']['ledger'].items():
                if (value['asset'] not in self.ledgerCodeToCommonCode):
                    self.log_it('error', 'Kraken ledger asset not recognized: {}'.format(str(value)))
                    continue

                value['ledgerid'] = key
                value['amount'] = float(value['amount'])
                value['fee'] = float(value['fee'])
                value['asset'] = self.ledgerCodeToCommonCode[value['asset']]
                reformattedItems.append(value)
            reformattedItems = pd.DataFrame(reformattedItems)

            if (len(reformattedItems) == 0):
                break

            # check if data is out of time range
            if (reformattedItems['time'].min() <= startTime):
                if (inclusive):
                    reformattedItems = reformattedItems[reformattedItems['time'] >= startTime]
                else:
                    reformattedItems = reformattedItems[reformattedItems['time'] > startTime]

                moreItems = False

            # accumulate trades
            allItems = allItems.append(reformattedItems, ignore_index=True)

        if (len(allItems) == 0):
            return []

        # convert timestamp to datetime
        allItems['time'] = pd.to_datetime(allItems['time'], unit='s')

        # rename columns (ccxt trade format)
        allItems = allItems.rename(columns={'time': 'datetime', 'asset': 'symbol'})

        # list of dicts
        temp = []
        for i, item in allItems.iterrows():
            item = item.to_dict()
            temp.append(item)
        allItems = temp

        return allItems

    def get_withdrawal_deposit_history(self, startTime, endTime, coinSymbols, inclusive=False):
        withdrawals=self.get_ledger_items(startTime, endTime, 'withdrawal', inclusive)
        deposits=self.get_ledger_items(startTime, endTime, 'deposit', inclusive)

        wds = withdrawals+deposits
        relevantWDs = []
        for wd in wds:
            if(wd['symbol'] in coinSymbols):
                relevantWDs.append(wd)

        return relevantWDs

    def withdraw(self, exchange_to):
        raise NotImplementedError("You need to implement this")

    def get_position_volume(self, market):
        # get open orders (last 30 days)
        totalVolume = 0
        openPositions = self.api.privatePostOpenPositions({'currencyPair': self.api.marketId(market)})['result']
        for positionID in openPositions:
            position = openPositions[positionID]

            if (position['pair'] != self.api.marketId(market)):
                continue

            if (position['type'] == 'sell'):
                totalVolume -= float(position['vol']) - float(position['vol_closed'])
            else:
                totalVolume += float(position['vol']) - float(position['vol_closed'])
        return totalVolume

    def margin_order_kraken_hard_code(self, market, quantity, side, leverage, useLimitOrders):
        allOrderInfo = []

        remaining = quantity

        if (useLimitOrders):
            while (remaining > 0):
                # place order
                if (side == 'buy'):
                    rate = self.api.fetch_ticker(symbol=market)['ask'] * (1 + .003)
                    order = self.api.create_limit_buy_order(market, remaining, rate, {'leverage': leverage})
                else:
                    rate = self.api.fetch_ticker(symbol=market)['bid'] * (1 - .003)
                    order = self.api.create_limit_sell_order(market, remaining, rate, {'leverage': leverage})

                # wait for order to fill
                for i in range(10):
                    time.sleep(1)
                    orderInfo = self.api.fetch_order(order['id'])
                    if (orderInfo['remaining'] == 0):
                        break

                remaining = orderInfo['remaining']

                # cancel order if it still hasn't filled
                if (remaining > 0):
                    cancelOrderInfo = self.api.cancel_order(orderInfo['id'])
                    orderInfo = self.api.fetch_order(order['id'])

                allOrderInfo.append(orderInfo)
        else:
            # place order
            if (side == 'buy'):
                order = self.api.create_market_buy_order(market, remaining, {'leverage': leverage})
            else:
                order = self.api.create_market_sell_order(market, remaining, {'leverage': leverage})

            allOrderInfo.append(self.api.fetch_order(order['id']))

        return allOrderInfo

    def get_otc_positions(self, markets):
        otcShortVolume = {}
        for market in markets:
            otcShortVolume[market] = 0
        return otcShortVolume

    def get_markets_with_margin_trading(self):
        markets = self.api.load_markets()
        leverageMarkets = []
        for market in markets:
            if (len(markets[market]['info']['leverage_buy']) > 0):
                leverageMarkets.append(market)
        return leverageMarkets

    def is_dust_order_error(self, e):
        return "EGeneral:Invalid arguments:volume" in str(e)

    def is_order_not_open_error(self, e):
        return "EOrder:Unknown order" in str(e)

    def adjust_short_kraken_hard_code(self, startSingleUnitShortPositions, endSingleUnitShortPositions, currencyUnit,
                                      useLimitOrders):
        markets = startSingleUnitShortPositions.keys()

        # Note: OTC positions hardcoded for now
        otcShortVolume = self.get_otc_positions(markets)

        tickers = self.api.fetch_tickers()
        startMultiUnitShortPositions = self.routeFinder.convert_single_unit_portfolio_to_multiple_unit_portfolio(
            startSingleUnitShortPositions, currencyUnit, tickers)
        endMultiUnitShortPositions = self.routeFinder.convert_single_unit_portfolio_to_multiple_unit_portfolio(
            endSingleUnitShortPositions, currencyUnit, tickers)

        actualMultiUnitShortPositions = {}

        adjustShortOrderInfo = {}

        for market in markets:
            # volume of short positions for market
            overallStartShortVolume = self.get_position_volume(market) - otcShortVolume[market]

            # get change in volume of short positions
            change = endMultiUnitShortPositions[market] - startMultiUnitShortPositions[market]

            if (change < 0):
                # short more of market
                adjustShortOrderInfo[market] = self.margin_order_kraken_hard_code(market, -change, 'sell', 2,
                                                                                  useLimitOrders)
            else:
                # partially cover existing shorts by going long
                adjustShortOrderInfo[market] = self.margin_order_kraken_hard_code(market, change, 'buy', 2,
                                                                                  useLimitOrders)

            overallEndShortVolume = self.get_position_volume(market) - otcShortVolume[market]

            actualMultiUnitShortPositions[market] = (overallEndShortVolume - overallStartShortVolume) + \
                                                    startMultiUnitShortPositions[market]

        tickers = self.api.fetch_tickers()
        actualSingleUnitShortPositions = self.routeFinder.convert_multiple_unit_portfolio_to_single_unit_portfolio(
            actualMultiUnitShortPositions, currencyUnit, tickers)

        return {'startPortfolio': startSingleUnitShortPositions, 'idealEndPortfolio': endSingleUnitShortPositions,
                'actualEndPortfolio': actualSingleUnitShortPositions}

    def get_past_orders(self, startTime, endTime, marketSymbols, inclusive=False):
        return self.get_past_trades(startTime, endTime, marketSymbols, inclusive)

    def get_past_trades(self, startTime, endTime, marketSymbols, inclusive=False):
        allTrades = pd.DataFrame()
        moreTrades = True

        # convert datetime to unix timestamp (seconds)
        startTime = startTime.timestamp()
        endTime = endTime.timestamp()
        temp = startTime
        startTime = endTime
        endTime = temp
        while moreTrades:
            endTime = allTrades['time'].min() - 1 if len(allTrades) else endTime

            self.log_it('info', "Loaded All trades since: {}".format(endTime))

            # get trade data + reformat to dataframe
            tradesRaw = run_function_safe(self.api.privatePostLedgers, {'type': 'trade', 'end': endTime})
            curTrades = []
            for key, value in tradesRaw['result']['ledger'].items():
                if (value['asset'] not in self.ledgerCodeToCommonCode):
                    continue
                value['ledgerid'] = key
                value['amount'] = float(value['amount'])
                value['asset'] = self.ledgerCodeToCommonCode[value['asset']]
                curTrades.append(value)
            curTrades = pd.DataFrame(curTrades)

            if (len(curTrades) == 0):
                break

            # check if trades are out of time range
            if (curTrades['time'].min() <= startTime):
                if (inclusive):
                    curTrades = curTrades[curTrades['time'] >= startTime]
                else:
                    curTrades = curTrades[curTrades['time'] > startTime]

                moreTrades = False

            # accumulate trades
            allTrades = allTrades.append(curTrades, ignore_index=True)

        if (len(allTrades) == 0):
            return []

        # convert timestamp to datetime
        allTrades['time'] = pd.to_datetime(allTrades['time'], unit='s')

        # rename columns (ccxt trade format)
        allTrades = allTrades.rename(columns={'time': 'datetime', 'amount': 'filled', 'asset': 'symbol'})

        # list of dicts
        temp = []
        for i, trade in allTrades.iterrows():
            trade = trade.to_dict()
            temp.append(trade)
        allTrades = temp

        marketNames = self.get_tickers_safe().keys()

        # match ledger entries to make trades
        matchedTrades = []
        matchedRefIds = set([])
        for trade0 in allTrades:
            for trade1 in allTrades:
                # same refid/ diff symbol/ hasn't been matched yet
                if (trade0['refid'] == trade1['refid'] and
                        trade0['refid'] not in matchedRefIds and
                        trade0['symbol'] != trade1['symbol']):

                    matchedRefIds.add(trade0['refid'])

                    matchedTrade = {}
                    if ('{}/{}'.format(trade0['symbol'], trade1['symbol']) in marketNames):
                        baseTrade = trade0
                        quoteTrade = trade1
                    elif ('{}/{}'.format(trade1['symbol'], trade0['symbol']) in marketNames):
                        baseTrade = trade1
                        quoteTrade = trade0
                    else:
                        raise NameError('Market does not exist: {}, {}'.format(trade0['symbol'], trade1['symbol']))

                    # symbol, datetime, filled, cost, fee, side, id
                    symbol = '{}/{}'.format(baseTrade['symbol'], quoteTrade['symbol'])
                    if (marketSymbols is None or symbol in marketSymbols):
                        matchedTrade['symbol'] = symbol
                        if (baseTrade['datetime'] > quoteTrade['datetime']):
                            matchedTrade['datetime'] = baseTrade['datetime']
                        else:
                            matchedTrade['datetime'] = quoteTrade['datetime']
                        if (float(quoteTrade['fee']) > 0):
                            matchedTrade['fee'] = {'cost': float(quoteTrade['fee']), 'currency': quoteTrade['symbol']}
                        else:
                            matchedTrade['fee'] = {'cost': float(baseTrade['fee']), 'currency': baseTrade['symbol']}
                        if (quoteTrade['filled'] > 0):
                            matchedTrade['side'] = 'sell'
                        else:
                            matchedTrade['side'] = 'buy'
                        matchedTrade['filled'] = abs(baseTrade['filled'])
                        matchedTrade['cost'] = abs(quoteTrade['filled'])
                        matchedTrade['id'] = baseTrade['refid']
                        matchedTrades.append(matchedTrade)


        return matchedTrades