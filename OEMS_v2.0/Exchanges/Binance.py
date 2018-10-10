import pandas as pd
from Exchanges.Base.Exchange import Exchange
from Base.Fees import Fees
from ccxt import binance
from utils.funcs import run_function_safe
import copy
from collections import defaultdict
import time
from Exchanges.Base.OrderBook import OrderBook


class Binance(Exchange):
    def __init__(self, keypath):
        with open(keypath, "r") as f:
            key = f.readline().strip()
            secret = f.readline().strip()
        self.useCoinapi = True
        Exchange.__init__(self, 'Binance', binance({'apiKey': key, 'secret': secret}))
        self.api.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        self.orderPlacementFreq = 0

    def get_balances(self, balanceDict = None):
        if(balanceDict is None):
            return run_function_safe(self.api.fetch_balance)
        else:
            balanceDict[self.exchange_name.lower()] = run_function_safe(self.api.fetch_balance)
            
    def trades_to_order(self, allTrades, order=None):
        if(order is None):
            realOrderInfo={}
        else:
            realOrderInfo=copy.deepcopy(order)
        maxTimestamp = 0
        costTotal = 0
        feeTotal = 0
        amount = 0
        for trade in allTrades:
            if (maxTimestamp < trade['timestamp']):
                maxTimestamp = trade['timestamp']
            costTotal += trade['cost']
            feeTotal += trade['fee']['cost']
            amount += trade['amount']
        if (maxTimestamp > 0):
            realOrderInfo['timestamp'] = maxTimestamp
            realOrderInfo['datetime'] = pd.Timestamp(maxTimestamp, unit='ms')
        else:
            realOrderInfo['datetime'] = pd.Timestamp(realOrderInfo['datetime']).tz_convert(None)

        realOrderInfo['filled'] = amount

        if(order is not None and
            order['filled'] > 0 and
            abs(order['filled']-amount)/order['filled']>.00001):
            self.log_it('error', '=======Order and Trades Do Not Match=======')
            for trade in allTrades:
                self.log_it('error',str(trade))
            self.log_it('error',str(order))
            raise NameError('Trades and order do not match.')

        if (amount > 0):
            realOrderInfo['price'] = costTotal / amount

        realOrderInfo['cost'] = costTotal

        if(order is not None):
            realOrderInfo['info']['allTrades'] = allTrades
        else:
            realOrderInfo['info']={'allTrades':allTrades}

        if (len(allTrades) == 0):
            realOrderInfo['fee'] = {'currency': order['symbol'].split('/')[1], 'cost': feeTotal}
        else:
            realOrderInfo['fee'] = {'currency': allTrades[0]['fee']['currency'], 'cost': feeTotal}
            realOrderInfo['side'] = allTrades[0]['side']
            realOrderInfo['symbol'] = allTrades[0]['symbol']
            realOrderInfo['id'] = allTrades[0]['order']

        return realOrderInfo

    def get_real_order_info(self, order):
        aggregate=0
        allTrades=[]
        tries=0
        curTimestamp = order['timestamp']
        while(order['filled'] != 0 and abs(order['filled']-aggregate)/order['filled']>.00001):
            tries+=1
            pastTrades = run_function_safe(self.api.fetchMyTrades, order['symbol'], curTimestamp)
            timestampsFromOrder = []
            allTimestamps = []
            for trade in pastTrades:
                if(trade['order'] == order['id']):
                    timestampsFromOrder.append(trade['timestamp'])
                    aggregate+=trade['amount']
                    allTrades.append(trade)
                allTimestamps.append(trade['timestamp'])
            if(len(timestampsFromOrder)>0):
                curTimestamp=max(timestampsFromOrder)+1
            elif(len(allTimestamps)>0):
                curTimestamp = max(allTimestamps)+1
            else:
                break

        return self.trades_to_order(allTrades,order)

    def limit_buy(self, market, amount, highest_rate, *args):
        return run_function_safe(self.api.create_limit_buy_order, market, amount, highest_rate, *args)

    def market_buy(self, market, amount, req=None):
        return run_function_safe(self.api.create_market_buy_order,market, amount)

    def limit_sell(self, market, amount, lowest_rate, *args):
        return run_function_safe(self.api.create_limit_sell_order, market, amount, lowest_rate, *args)

    def market_sell(self, market, amount, req=None):
        return run_function_safe(self.api.create_market_sell_order, market, amount)

    def get_quote_spent_or_gained(self, order_info):
        if(order_info['side']=='buy'):
            return order_info['cost']+order_info['fee']['cost']
        else:
            return order_info['cost']-order_info['fee']['cost']

    def choose_route(self, routes):
        #choose shortest route and avoid ETH markets if possible
        allRouteLens = list(map(lambda x: len(x), routes))
        minRouteLength = min(allRouteLens)
        hasETH = list(map(lambda x: self.routeFinder.uses_quote_market(x, 'ETH'), routes))
        hasBNB = list(map(lambda x: self.routeFinder.uses_quote_market(x, 'BNB'), routes))

        for i, route in enumerate(routes):
            if(len(route)==minRouteLength and not(hasETH[i]) and not(hasBNB[i])):
                return route
        
        chosenRoute = routes[allRouteLens.index(minRouteLength)]
        return chosenRoute

    def cancel_order(self, order_id, symbol):
        return run_function_safe(self.api.cancel_order, order_id, symbol)

    def fetch_open_orders(self, symbols):
        if(symbols is None):
            return run_function_safe(self.api.fetch_open_orders)
        else:
            orders=[]
            for symbol in symbols:
                orders+=run_function_safe(self.api.fetch_open_orders,symbol)
            return orders

    def fetch_closed_orders(self, symbols):
        orders=[]
        for symbol in symbols:
            orders+=run_function_safe(self.api.fetchClosedOrders, symbol)
        return orders

    def cancel_open_orders(self, symbols):
        cancelOrderInfo = []
        openOrders = self.fetch_open_orders(symbols)
        for orderInfo in openOrders:
            cancelOrderInfo.append(self.cancel_order(self.get_order_id(orderInfo), orderInfo['symbol']))
        return {'result':cancelOrderInfo}

    def get_ob(self, market, depth=100):
        obUnformatted = run_function_safe(self.api.fetch_order_book, market)
        obs = OrderBook(obUnformatted['bids'], obUnformatted['asks'])
        return obs

    def get_order_stats(self, order_id, timestamp=None, marketSym=None):
        return run_function_safe(self.api.fetch_order ,order_id, marketSym)

    def get_order_id(self, orderInfo):
        return orderInfo['id']

    #todo...
    def update_fees(self):
        for market in run_function_safe(self.api.fetch_markets):
            pair = market['symbol']
            self.fees[pair] = Fees()

    def get_fees(self, _):
        return Fees()

    def is_market_offline_error(self, e):
        return "Market is closed" in str(e)

    def is_dust_order_error(self, e):
        return "Filter failure: MIN_NOTIONAL" in str(e) or "Invalid quantity." in str(e)

    def is_order_not_open_error(self, e):
        return "UNKNOWN_ORDER" in str(e)

    # {u'depositList': [{u'status': 1, u'addressTag': u'100883956',
    #                    u'txId': u'9BC131DFAFF7B4E74C25555A3F56EF838024F30BD6D2EE417B2CAB0CCF10467C',
    #                    u'amount': 71999.98, u'asset': u'XRP', u'insertTime': 1525231632000,
    #                    u'address': u'rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh'}], u'success': True}

    # deposits = bina.wapi_get_deposithistory(
    #     {'asset': 'XRP', 'startTime': int((pd.Timestamp.now() - pd.Timedelta(days=10)).timestamp() * 1000),
    #      'endTime': int(time.time() * 1000), 'timestamp': int(time.time() * 1000)})

    #todo check inclusion
    def gather_wds(self, coin, startTime, endTime, wdType, inclusive=False):
        moreWds = True
        allWds=[]
        while(moreWds):
            if(wdType=='deposit'):
                wds = self.api.wapi_get_deposithistory({'asset': coin,
                                                  'startTime': startTime,
                                                  'endTime': endTime,
                                                  'timestamp': int(time.time() * 1000)})['depositList']
            elif(wdType=='withdraw'):
                wds = self.api.wapi_get_withdrawhistory({'asset': coin,
                                                             'startTime': startTime,
                                                             'endTime': endTime,
                                                             'timestamp': int(time.time() * 1000)})['withdrawList']
            else:
                raise NameError('Invalid wdType value: {}'.format(wdType))

            if(len(wds)> 0):
                wdsReformatted = []
                lastTime = 0
                for wd in wds:
                    #get time
                    if(wdType=='deposit'):
                        wdTime = wd['insertTime']
                    else:
                        wdTime = wd['applyTime']

                    #keep track of most recent w/d in case api call didn't abide by given bounds
                    if(wdTime > lastTime):
                        lastTime = wdTime

                    #check bounds
                    if((inclusive and wdTime <= endTime and wdTime >= startTime) or
                        (not(inclusive) and wdTime < endTime and wdTime > startTime)):

                        #reformat wd
                        wdReformatted = {}
                        wdReformatted['datetime'] = pd.Timestamp(wdTime, unit='ms')
                        wdReformatted['symbol'] = wd['asset']
                        if(wdType == 'deposit'):
                            wdReformatted['amount'] = wd['amount']
                        else:
                            wdReformatted['amount'] = -wd['amount']

                        wdsReformatted.append(wdReformatted)

                print(wdsReformatted)
                #quick check to save an api call
                if(len(wdsReformatted)==0 or lastTime > endTime):
                    moreWds = False
                else:
                    startTime = lastTime+1
                    allWds+=wdsReformatted
            else:
                moreWds = False

        return allWds

    def get_withdrawal_deposit_history(self, startTime, endTime, coinSymbols, inclusive=False):
        startTime = int(startTime.timestamp()*1000)
        endTime = int(endTime.timestamp()*1000)
        wds=[]
        for coin in coinSymbols:
            wds+=self.gather_wds(coin, startTime, endTime, 'deposit', inclusive)
            wds+=self.gather_wds(coin, startTime, endTime, 'withdraw', inclusive)

        return wds

    def withdraw(self, exchange_to):
        raise NotImplementedError("You need to implement this")

    def get_past_orders(self, startTime, endTime, marketSymbols, inclusive=False):
        trades = self.get_past_trades(startTime, endTime, marketSymbols, inclusive)

        orderDict=defaultdict(list)
        for trade in trades:
            orderDict[str(trade['order'])+str(trade['symbol'])].append(trade)
        orders=[]
        for key in orderDict:
            orders.append(self.trades_to_order(orderDict[key], None))
        return orders

    def get_past_trades(self, startTime, endTime, marketSymbols, inclusive=False):
        allTrades = []
        startTime=startTime.timestamp()*1000
        endTime=endTime.timestamp()*1000

        for marketSymbol in marketSymbols:
            marketSymStartTime=startTime
            fetchMoreTrades=True
            while(fetchMoreTrades):
                #get tradebatch
                tradeBatch=run_function_safe(self.api.fetchMyTrades, marketSymbol, marketSymStartTime)
                tradeBatch=pd.DataFrame(tradeBatch)

                if(len(tradeBatch) == 0):
                    break

                if(not(inclusive) or marketSymStartTime != startTime):
                    tradeBatch = tradeBatch[tradeBatch['timestamp'] > marketSymStartTime]

                if (len(tradeBatch) == 0):
                    break

                if(tradeBatch['timestamp'].max() < endTime):
                    self.log_it('info', 'Fetched {} {} trades since {}.'.format(len(tradeBatch),
                                                                            marketSymbol,
                                                                            pd.Timestamp(marketSymStartTime,unit='ms')))
                    marketSymStartTime=tradeBatch['timestamp'].max()
                else:
                    fetchMoreTrades=False

                    if(not inclusive):
                        tradeBatch=tradeBatch[tradeBatch['timestamp']<endTime]
                    else:
                        tradeBatch=tradeBatch[tradeBatch['timestamp']<=endTime]

                    self.log_it('info', 'Fetched {} {} trades since {}.'.format(len(tradeBatch),
                                                                                marketSymbol,
                                                                                pd.Timestamp(marketSymStartTime,
                                                                                             unit='ms')))

                #store tradebatch
                temp = []
                for i,trade in tradeBatch.iterrows():
                    trade = trade.to_dict()
                    temp.append(trade)
                allTrades += temp

        return allTrades