from ccxt import bitflyer
import time
import pandas as pd
import dotenv
variables = dotenv.get_variables('.env')

def getenv(key):
    return variables[key]


class BitFlyerTracer:
    def __init__(self):
        self.api = bitflyer({
            'apiKey': getenv('bitflyer_api_key'),
            'secret': getenv('bitflyer_api_secret')
        })

    def get_coins_in(self):
        return pd.DataFrame(self.api.privateGetGetcoinins())

    def get_coins_out(self):
        return pd.DataFrame(self.api.privateGetGetcoinouts())

    def get_addresses(self):
        return pd.DataFrame(self.api.privateGetGetaddresses())

    def get_balance(self):
        return pd.DataFrame(self.api.fetch_balance()['info'])


if __name__ == '__main__':
    b = BitFlyerTracer()
    start = time.time()
    b.get_coins_in().to_csv('bitflyer_data/deposits.{}.csv'.format(int(start)), index=False)
    time.sleep(1)  # don't want to get rate limited
    b.get_coins_out().to_csv('bitflyer_data/withdrawals.{}.csv'.format(int(start)), index=False)
    time.sleep(1)  # don't want to get rate limited
    b.get_addresses().to_csv('bitflyer_data/addresses.{}.csv'.format(int(start)), index=False)
    time.sleep(1)
    b.get_balance().to_csv('bitflyer_data/balance.{}.csv'.format(int(start)), index=False)
