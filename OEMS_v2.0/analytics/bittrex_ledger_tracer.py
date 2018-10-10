from ccxt import bittrex
from ccxt.base import errors
import time
import pandas as pd
import dotenv
import os
variables = dotenv.get_variables('.env')

def getenv(key):
    return variables[key]

class BittrexTracer:
    def __init__(self):
        self.api = bittrex({
            'apiKey': getenv('bittrex_api_key'),
            'secret': getenv('bittrex_api_secret')
        })

    def get_coins_in(self):
        df = pd.DataFrame()
        currencies = self.api.fetch_balance()['info']
        for market in currencies:
            market = market['Currency']
            try:
                withdrawals = pd.DataFrame(self.api.accountGetWithdrawalhistory({'currency': market})['result'])
                df = df.append(withdrawals, ignore_index=True)
            except (errors.ExchangeError, errors.RequestTimeout):
                pass
            time.sleep(1)
        return df

    def get_coins_out(self):
        df = pd.DataFrame()
        currencies = self.api.fetch_balance()
        for market in currencies.keys():
            try:
                withdrawals = pd.DataFrame(self.api.accountGetDeposithistory({'currency': market})['result'])
                df = df.append(withdrawals, ignore_index=True)
            except (errors.ExchangeError, errors.RequestTimeout):
                pass
            time.sleep(1)
        return df

    def get_addresses(self):
        return pd.DataFrame(self.api.accountGetBalances()['result'])

    def get_balance(self):
        bal = self.api.accountGetBalances()['result']
        return pd.DataFrame(bal)

    def get_trades(self):
        df = pd.DataFrame()
        currencies = self.api.fetch_balance()
        for market in currencies.keys():
            try:
                withdrawals = pd.DataFrame(self.api.accountGetOrderhistory({'currency': market})['result'])
                df = df.append(withdrawals, ignore_index=True)
            except (errors.ExchangeError, errors.RequestTimeout):
                pass
            time.sleep(.5)
        return df


if __name__ == '__main__':
    b = BittrexTracer()
    start = time.time()
    coins_in = b.get_coins_in() 
    time.sleep(1)  # don't want to get rate limited
    coins_out = b.get_coins_out()
    time.sleep(1)  # don't want to get rate limited
    addresses = b.get_addresses() 
    time.sleep(1)
    balance = b.get_balance()  
    time.sleep(1)
    b.get_trades().to_csv('bittrex_data/trade.{}.csv'.format(int(start)),index=False)
    coins_in.to_csv('bittrex_data/coins_in.{}.csv'.format(int(start)), index=False)
    coins_out.to_csv('bittrex_data/coins_out.{}.csv'.format(int(start)), index=False)
    balance.to_csv('bittrex_data/balance.{}.csv'.format(int(start)), index=False)
    addresses.to_csv('bittrex_data/addresses.{}.csv'.format(int(start)), index=False)
