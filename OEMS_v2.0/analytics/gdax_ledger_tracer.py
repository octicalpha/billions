from ccxt import gdax
import time
import pandas as pd
import datetime
import dotenv
variables = dotenv.get_variables('.env')

def getenv(key):
    return variables[key]


class GDAXTracer:
    def __init__(self):
        self.api = gdax({
            'apiKey': getenv('gdax_api_key'),
            'secret': getenv('gdax_api_secret'),
            'password': getenv('gdax_api_passphrase')
        })
        self.accounts = pd.DataFrame(self.api.privateGetAccounts())

    def send_fills_and_accounts(self, email):
        for account_id in self.accounts.id.values:
            self.api.privatePostReports({'email': email, 'type': 'account', 'account_id': account_id})

        for market in self.api.fetch_markets():
            self.api.privatePostReports({'email': email, 'type': 'fills', 'product_id': market['id']})

    def get_balance(self):
        return pd.DataFrame(self.api.fetch_balance()['info'])

    def get_withdrawals(self):
        pass

    def get_deposits(self):
        pass

    def get_trades(self):
        pass


if __name__ == '__main__':
    sent_email = False
    g = GDAXTracer()
    start = time.time()
    g.get_balance().to_csv('gdax_data/balance.{}.csv'.format(int(start)), index=False)
    time.sleep(1)  # don't want to get rate limited
    now = datetime.datetime.now()
    time_diff = (now - now.replace(day=1, hour=1, minute=1, second=1, microsecond=1)).total_seconds()
    if time_diff < 60*5 and not sent_email:
        g.send_fills_and_accounts('AltcoinAdmin@tridenttrust.us')
        sent_email = True
    if time_diff >= 60*5:
        sent_email = False
