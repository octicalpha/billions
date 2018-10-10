from bittrex import Bittrex as _Bittrex
class BittrexWrapper:
    def __init__(self,api_key,api_secret):
        # Bittrex Account Setup #
        self.__bittrex = _Bittrex(api_key, api_secret)
        self.bid_ask = ['Bid', 'Ask']
        # Bittrex Account Setup #
    
    def get_bittrex_balance(self):
        r = self.__bittrex.get_balances()
        if r['success']:
            return r["result"]
        else:
            print("There was an error with the result: %s" % r["message"])

    def get_market_summaries_dict(self):
        market_summaries_dict = {}
        market_summaries = self.__bittrex.get_market_summaries()['result']
        for market_summary in market_summaries:
            key = market_summary['MarketName']
            market_summaries_dict[key] = market_summary
        return market_summaries_dict

    def get_bittrex_value(self, market_summaries_dict):
        bittrex_balances = self.get_bittrex_balance()
        bittrex_values = self.get_bittrex_values(bittrex_balances, market_summaries_dict)
        bittrex_value = sum(bittrex_values.values())
        return bittrex_value

    def buy_market(self,market,amount,highest_rate,amount_offset=.05):
        return self.__bittrex.buy_limit(market,  amount, float(highest_rate) * (1 + amount_offset))

    def sell_market(self,market,amount,lowest_rate,amount_offset=.05):
        return self.__bittrex.sell_limit(market, amount, float(lowest_rate)  * (1 - amount_offset))

    def get_bittrex_obj(self):
        return self.__bittrex

    def get_orderbook(self,market,depth_type='both'):
        return self.__bittrex.get_orderbook(market,depth_type).get('result',{})
"""
if __name__ == '__main__':
    API_KEY = "7b1c6c4db0664caba1ae4d5fc355481b"
    API_SECRET = "5dee429576694e5198afda250cede8e2"
    b = BittrexWrapper(API_KEY,API_SECRET)
    data = b.get_order_book('BTC-LTC')
    print data
    data = data.get('result',{})

    import pandas as pd
    df = pd.DataFrame(data=data.get('sell'))
    print df.columns
    print df.iloc[0]
    df = pd.DataFrame(data=data.get('buy'))
    print df.columns
    print df.iloc[0]
    """
