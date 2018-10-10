from Exchanges.Base.Exchange import Exchange
from ccxt import gdax


class GDAX(Exchange):
    def __init__(self, keypath):
        with open(keypath, "r") as f:
            passphrase = f.readline().strip()
            key = f.readline().strip()
            secret = f.readline().strip()
        Exchange.__init__(self, 'Gdax', gdax({'apiKey': key, 'secret': secret, 'password': passphrase}))

    def get_balances(self):
        return self.api.fetch_balance()

    def limit_buy(self, market, amount, highest_rate, *args):
        return self.api.create_limit_buy_order(market, amount, highest_rate, *args)

    def market_buy(self, market, amount, highest_rate, req=None):
        return self.api.create_market_buy_order(market, amount, params=req)

    def limit_sell(self, market, amount, lowest_rate, *args):
        return self.api.create_limit_sell_order(market, amount, lowest_rate, *args)

    def market_sell(self, market, amount, lowest_rate, req=None):
        return self.api.create_market_sell_order(market, amount, params=req)

    def get_ob(self, market, depth=100):
        return self.api.fetch_order_book(market, count=depth)

    def format_ob(self, ob):
        # TODO: This
        return ob

    def get_order_stats(self, order_id, timestamp):
        raise NotImplementedError("You need to implement this")

    def get_fees(self, pair):
        # TODO: don't think there is an API for this
        if 'LTC' in pair or 'ETH' in pair:
            taker = .003
        elif 'BCH' in pair or 'BTC' in pair:
            taker = .0025
        else:
            raise KeyError("Not sure for pair: {}".format(pair))
        return {'maker': 0.0, 'taker': taker}

    def withdraw(self, exchange_to):
        raise NotImplementedError("You need to implement this")
