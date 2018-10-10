import time
from collections import defaultdict
from utils.postgresConnection import storeInDb,query
from get_data_fns import get_coins,get_history_of_coin,join_history_data

if __name__ == '__main__':
    coins = get_coins()
    ts_df = query('select distinct ts, symbol from coincap.history', environment='aws_dev', dataframe=True)
    unix_ts_already_done_dict = defaultdict(set)
    for coin in set(ts_df['symbol'].values):
        unix_ts_already_done_dict[coin] = set(ts_df[ts_df['symbol'] == coin]['ts'].values)
    while True:
        for coin in get_coins():
            data = get_history_of_coin(coin)
            if data is None:
                continue
            df = join_history_data(data)
            df['symbol'] = coin
            ts_already_done = unix_ts_already_done_dict[coin]
            df = df[ ~ df['ts'].isin(ts_already_done) ] # take away duplicates
            unix_ts_already_done_dict[coin] |= set(df['ts'].values) # union
            storeInDb(df,'history','aws_dev',schema='coincap')
        time.sleep(2.5)

