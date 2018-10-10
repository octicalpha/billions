from ccxt import kraken
import time
import pandas as pd
from ccxt.base import errors
import logging
import dotenv
import os
variables = dotenv.get_variables('.env')

def getenv(key):
    return variables[key]



logger = logging.getLogger("kraken_tracer.log")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='%(asctime)s::[%(levelname)s]:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
ch = logging.FileHandler("kraken_tracer.log", mode='a')
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


class KrakenTracer:
    def __init__(self):
        self.k = kraken({'apiKey': getenv('kraken_api_key'),
                         'secret': getenv('kraken_api_secret')
                         }
                        )

    def get_deposit_withdrawals(self, since):
        def process_data(func, all_data_df):
            data_raw = func()
            data = []
            for key, value in data_raw['result']['ledger'].items():
                value['ledgerid'] = key
                data.append(value)
            df = pd.DataFrame(data)
            all_data_df = all_data_df.append(df, ignore_index=True)
            min_time = df['time'].min()
            if min_time < since:
                all_data_df = all_data_df[all_data_df['time'] >= since]
                return False, all_data_df
            return True, all_data_df

        all_df = pd.DataFrame()

        still_running = True
        while still_running:
            end = all_df['time'].min() - 1 if len(all_df) else time.time()
            logger.info("at end for all ledger: {}".format(end))
            still_running, all_df = process_data(lambda: self.k.privatePostLedgers({'type': 'all', 'end': end}), all_df)
        withdrawal_df = all_df[all_df['type'] == 'withdrawal']
        deposits_df = all_df[all_df['type'] == 'deposit']

        return deposits_df, withdrawal_df, all_df

    def get_trade_history(self, since):
        trade_df = pd.DataFrame()
        still_running = True
        while still_running:
            end = trade_df['time'].min() - 1 if len(trade_df) else time.time()

    def deposits(self, asset):
        data = self.k.privatePostDepositstatus({'asset': asset})
        df = pd.DataFrame(data['result'])
        if len(df) == 0:
            return pd.DataFrame()
        elif 'status' not in df.columns:
            logger.error("Status not in df")
            logger.error(df)
            raise ValueError("status not in df")
        return df[ df['status'] == 'Success']

    def withdrawals(self, asset):
        data = self.k.privatePostWithdrawstatus({'asset': asset})
        df = pd.DataFrame(data['result'])
        if len(df) == 0:
            return pd.DataFrame()
        elif 'status' not in df.columns:
            logger.error("Status not in df")
            logger.error(df)
            raise ValueError("status not in df")
        return df[df['status'] == 'Success']

    def reconcile(self, assets, since):
        deposits_meta, withdrawals_meta, all_df = self.get_deposit_withdrawals(since)

        withdrawals_reconciled = pd.DataFrame()
        deposits_reconciled = pd.DataFrame()
        for asset in assets:
            time.sleep(1)
            logger.info("at asset",asset)
            try:
                withdrawalsdata = self.withdrawals(asset)
            except errors.RequestTimeout as RT:
                logger.error("There was an error for withdrawal {}. {}".format(asset, RT))
            else:
                if len(withdrawalsdata):
                    withdrawals_only_asset = withdrawals_meta[withdrawals_meta['asset'] == asset]
                    cols = withdrawals_only_asset.columns.difference(withdrawalsdata.columns.difference(['refid']))
                    df = pd.merge(withdrawalsdata, withdrawals_only_asset[cols], on='refid', how='left')
                    withdrawals_reconciled = withdrawals_reconciled.append(df, ignore_index=True)

            try:
                depositdata = self.deposits(asset)
            except errors.RequestTimeout as RT:
                logger.error("There was an error for deposit {}. {}".format(asset, RT))
            else:
                if len(depositdata) == 0:
                    continue
                deposit_only_asset = deposits_meta[deposits_meta['asset'] == asset]
                cols = deposit_only_asset.columns.difference(depositdata.columns.difference(['refid']))
                df = pd.merge(depositdata, deposit_only_asset[cols], on='refid', how='left')
                deposits_reconciled = deposits_reconciled.append(df, ignore_index=True)

        return withdrawals_reconciled, deposits_reconciled, all_df

    def balance(self):
        balance = self.k.fetch_balance()
        del balance['info']
        data = []
        for k, v in balance.items():
            v['currency'] = k
            data.append(k)
        return pd.DataFrame(data)

    def get_addresses(self):
        assets = []
        # TODO: get assets
        for asset in assets:
            self.k.privateGetDepositaddresses()
            # TODO: do something with this

if __name__ == '__main__':
    k = KrakenTracer()
    since = int(time.time() - 60 * 60 * 24 * 31)  # last 31 days
    d, w, all_df = k.reconcile(['XETH', 'XXBT', 'XETC', 'BCH', 'LTC', 'XRP', 'ZUSD'], since)
    end = int(time.time())
    d.to_csv('kraken_data/deposits.{}.{}.csv'.format(since, end), index=False)
    w.to_csv('kraken_data/withdrawals.{}.{}.csv'.format(since, end), index=False)
    all_df.to_csv('kraken_data/ledger.{}.{}.csv'.format(since, end), index=False)
    time.sleep(1)
    k.balance().to_csv('kraken_data/balance.{}.{}.csv'.format(since, end), index=False)

