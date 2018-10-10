import pandas as pd
from Exchanges.Base.Exchange import Exchange
from Base.Fees import Fees
from ccxt import bittrex
from utils.funcs import run_function_safe
from Exchanges.Base.OrderBook import OrderBook

class Bittrex(Exchange):
    def __init__(self, keypath):
        with open(keypath, "r") as f:
            key = f.readline().strip()
            secret = f.readline().strip()
        self.useCoinapi = False
        self.orderPlacementFreq = 1
        Exchange.__init__(self, 'Bittrex', bittrex({'apiKey': key, 'secret': secret}))

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
        return run_function_safe(self.api.create_market_buy_order,market, amount, params=req)

    def limit_sell(self, market, amount, lowest_rate, *args):
        return run_function_safe(self.api.create_limit_sell_order, market, amount, lowest_rate, *args)

    def market_sell(self, market, amount, req=None):
        return run_function_safe(self.api.create_market_sell_order, market, amount, params=req)

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

        for i, route in enumerate(routes):
            if(len(route)==minRouteLength and not(hasETH[i])):
                return route

        return routes[allRouteLens.index(minRouteLength)]

    def cancel_order(self, order_id, symbol=None):
        return run_function_safe(self.api.cancel_order, order_id)

    #todo integrate symbols
    def fetch_open_orders(self, symbols=None):
        return run_function_safe(self.api.fetch_open_orders)

    def fetch_closed_orders(self, symbols=None):
        return run_function_safe(self.api.fetch_closed_orders)

    def cancel_open_orders(self, symbols):
        cancelOrderInfo = []
        openOrders = self.fetch_open_orders()
        for orderInfo in openOrders:
            cancelOrderInfo.append(self.cancel_order(self.get_order_id(orderInfo)))
        return {'result':cancelOrderInfo}

    def get_ob(self, market, depth=100):
        obs = run_function_safe(self.api.fetch_order_book, market)
        obs = OrderBook(obs['bids'], obs['asks'])
        return obs

    def format_ob(self, ob):
        formatted = pd.DataFrame(ob, columns=['price', 'amount'])
        formatted['order_id'] = None
        return formatted

    def get_order_stats(self, order_id, timestamp=None, marketSym=None):
        return run_function_safe(self.api.fetch_order,order_id)

    def get_order_id(self, orderInfo):
        return orderInfo['id']

    def update_fees(self):
        for market in run_function_safe(self.api.fetch_markets):
            pair = market['symbol']
            self.fees[pair] = Fees()

    def get_fees(self, _):
        return Fees()

    def is_dust_order_error(self, e):
        return "DUST_TRADE_DISALLOWED_MIN_VALUE_50K_SAT" in str(e) or "MIN_TRADE_REQUIREMENT_NOT_MET" in str(e) or "QUANTITY_NOT_PROVIDED" in str(e)

    def is_market_offline_error(self, e):
        return "MARKET_OFFLINE" in str(e)

    def is_order_not_open_error(self, e):
        return "ORDER_NOT_OPEN" in str(e)

    def withdraw(self, exchange_to):
        raise NotImplementedError("You need to implement this")

    def get_past_orders(self, startTime, endTime, marketSymbols, inclusive=False):
        orders=self.fetch_closed_orders()
        startTime=startTime.timestamp()*1000
        endTime=endTime.timestamp()*1000
        if(inclusive):
            orders = [order for order in orders if(order['timestamp']>=startTime and order['timestamp']<=endTime)]
        else:
            orders = [order for order in orders if(order['timestamp']>startTime and order['timestamp']<endTime)]
        return orders