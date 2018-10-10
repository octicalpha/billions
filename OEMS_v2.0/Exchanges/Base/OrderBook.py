import time

class OrderBookLevel:
    def __init__(self, price_quantity):
        self.price = price_quantity[0]
        self.quantity = price_quantity[1]

class OrderBook:
    def __init__(self, bids=None, asks=None):
        if(bids is not None):
            for i in range(len(bids)):
                bids[i] = OrderBookLevel(bids[i])
        if(asks is not None):
            for i in range(len(asks)):
                asks[i] = OrderBookLevel(asks[i])

        self.bids = bids
        self.asks = asks
        self.last_updated = time.time()

    def add_market_price(self, fees_obj):
        taker_fee = fees_obj.taker
        self.bids['market_price_fee'] = self.bids['price'] * taker_fee
        self.bids['market_price'] = self.bids['price'] - self.bids['market_price_fee']
        self.asks['market_price_fee'] = self.asks['price'] * taker_fee
        self.asks['market_price'] = self.asks['price'] + self.asks['market_price_fee']


class TooLightException(Exception):
    """Raise when OrderBook is too light"""

class LiquidityException(Exception):
    """Raise when market is too illiquid"""