import utils.postgresConnection as pgConn
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os
import requests
import datetime
from datetime import datetime, timedelta
import time
import argparse

exchanges_dict = {'BTRX': 'BITTREX',
                  'KBIT': 'KORBIT',
                  'GDAX': 'COINBASE',
                  'BTHM': 'BITHUMB',
                  'CONE': 'COINONE',
                  'GMNI': 'GEMINI',
                  'KRKN': 'KRAKEN',
                  'PLNX': 'POLONIEX',
                  'BINA': 'BINANCE'}
ex_coin = ['BLOCK', 'BNT', 'BTCD', 'ENG', 'FCT', 'GBYTE', 'GNO', 'MAID', 'MANA', 'PART', 'REP']
path = ''


def get_universe(is_csv, d, top, uni_time, exchange_list):
    sql = "select * from universe.mktcap_d{}_top{} where time <= '{}' order by time desc fetch first 5 rows only;"
    # print(sql)
    try:
        uni_time = datetime.strftime(uni_time, '%Y-%m-%d %H:%M:%S')
        data = pgConn.query(sql.format(d, top, uni_time), environment="aws_dev", dataframe=True)
    except:
        print('universe error')
    # exchange_list = ['BTRX']

    universe_symbol_all = []
    length_d = 5

    for i, symbol in enumerate(data.columns):
        tf = [data.values[j][i] for j in range(5)]
        if 'true' in tf:
            base, quote, exchange = symbol.split('-')
            if (exchange in exchange_list) and (base not in ex_coin) and (quote not in ex_coin):
                if is_csv:
                    if exchange in ['KRKN']:
                        if quote in ['USD']:
                            universe_symbol_all.append(data.columns[i])
                    elif exchange in ['BINA']:
                        universe_symbol_all.append('XRP-USDT-BINA')
                        if quote in ['BTC', 'USDT']:
                            universe_symbol_all.append(data.columns[i])
                    elif exchange in ['GDAX']:
                        if quote in ['BTC', 'USD']:
                            universe_symbol_all.append(data.columns[i])
                    else:
                        universe_symbol_all.append(data.columns[i])
                else:
                    if exchange in ['BINA']:
                        universe_symbol_all.append('XRP-USDT-BINA')
                        if quote in ['BTC', 'USDT']:
                            universe_symbol_all.append(data.columns[i])
                    elif exchange in ['GDAX']:
                        if quote in ['BTC', 'USD']:
                            universe_symbol_all.append(data.columns[i])
                    else:
                        universe_symbol_all.append(data.columns[i])
    '''    
    for day in range(length_d):
        for i, symbol in enumerate(data.values[day]):
            if symbol in ['true']:
                base, quote, exchange = data.columns[i].split('-')
                if (exchange in exchange_list) and (base not in ex_coin) and (quote not in ex_coin):
                    universe_symbol_all.append(data.columns[i])
    '''
    # TODO erase after coinapi update
    # universe_symbol.append('XRP-USD-KRKN')

    ###
    universe_symbol = list(set(universe_symbol_all))


    print(universe_symbol)
    print(len(universe_symbol))

    return universe_symbol


def get_coinapi_data(exchangeName, marketName, startTime, endTime, limit, coinapiKey):
    marketSym = '_'.join([exchangeName, marketName])
    # print(marketSym)
    startTime = datetime.strftime(startTime, '%Y-%m-%dT%H:%M:%SZ')
    endTime = datetime.strftime(endTime, '%Y-%m-%dT%H:%M:%SZ')
    try:
        skeleton = 'https://rest-us-east-altcoinadvisors.coinapi.io/v1/ohlcv/{}/history?period_id=1MIN&time_start={}&time_end={}&limit={}'
        headers = {'X-CoinAPI-Key': coinapiKey}
        res = requests.get(skeleton.format(marketSym, startTime, endTime, limit), headers=headers)
        # df = pd.DataFrame(requests.get(skeleton.format(marketSym, startTime, endTime, limit), headers=headers).json())

        print(res.headers['X-RateLimit-Limit'] + '/' + res.headers['X-RateLimit-Remaining'] + '/' + res.headers['X-RateLimit-Request-Cost'])

        # print(res.headers['X-RateLimit-Reset'])
        if int(res.headers['X-RateLimit-Request-Cost']) > 0:
            df = pd.DataFrame(res.json())
            # print(df)

            df['time_period_start'] = pd.to_datetime(df['time_period_start'])
        else:
            return None
    except Exception as e:
        print(marketSym + e)
    return df


def get_last_time_db(schemaName, tableName, environment, startTime):
    if (pgConn.table_exits(schemaName, tableName, environment)):
        maxTime = pgConn.query('select max(time) from "{}"."{}"'.format(schemaName, tableName), environment=environment, dataframe=True)['max'][0]

        if (maxTime is not None):
            maxTime += pd.Timedelta(minutes=1)
            return maxTime

    return startTime


def get_last_time_csv(df):
    if len(df) > 0:
        date = df.values[len(df) - 1]
    else:
        return None

    if date is not None:
        date = pd.Timestamp(date)
        date += pd.Timedelta(minutes=1)
    return date


def reformat_data(data):
    oldColsToUse = ['time_period_start', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_traded', 'trades_count']
    newColNames = ['time', 'open', 'high', 'low', 'close', 'baseVolume', 'tradeCount']
    data = data[oldColsToUse]
    nameMapping = {}
    for i in range(len(oldColsToUse)):
        nameMapping[oldColsToUse[i]] = newColNames[i]
    data = data.rename(columns=nameMapping)
    return data


def get_and_store_ohlcv_data(is_csv, exchangeSymbol, marketName, schemaName, tableName, environment, startTime, endTime, limit, coinapiKey):
    # if table exists
    # startTime = get_last_time(schemaName, tableName, environment, startTime)
    # print(marketName, exchangeSymbol)
    exsiting = False
    try:
        _, base, quote = marketName.split('_')
        if is_csv:
            # folder = '/home/ubuntu/codes/AltSim/2.0/data/ohlcv/{}/{}-OHLCV_SPOT_{}_{}.csv'
            filepath = path + '{}/{}-OHLCV_SPOT_{}_{}.csv'
            filename = filepath.format(schemaName, schemaName, base, quote)
            # TODO erase after coinapi update
            # if (base == "XRP") and (quote == "USD") and (schemaName == "KRKN"):
            #    filename = filepath.format("BINA", "BINA", "XRP", "USDT")
            ###
            if (os.path.isfile(filename)):
                ohlcv_data = pd.read_csv(filename, index_col=0)
                tempTime = startTime
                startTime = get_last_time_csv(ohlcv_data['time'])
                if startTime is not None:
                    exsiting = True
                else:
                    startTime = tempTime

        else:
            startTime = get_last_time_db(schemaName, tableName, environment, startTime)

        print(schemaName, tableName, startTime)
        if startTime >= endTime:
            return False
        # get_coinapi_data

        data = get_coinapi_data(exchangeSymbol, marketName, startTime, endTime, limit, coinapiKey)

        if data is None:
            return False
        # reformat data (time, open, high, low, close, baseVolume, tradesCount)
        data = reformat_data(data)

        if is_csv:
            data['in_z'] = pd.to_datetime(time.strftime("%Y%m%dT%H%M%S"))

            if exsiting:
                ohlcv_data = ohlcv_data.append(data, ignore_index=True)
                ohlcv_data.to_csv(filename)
            else:
                data.to_csv(filename)

        else:
            pgConn.storeInDb(data, tableName, environment, schema=schemaName)

        return limit == len(data)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    load_dotenv(find_dotenv())
    coinapiKey = os.environ.get('ALTCOIN_COINAPI_KEY')
    headers = {'X-CoinAPI-Key': coinapiKey}

    parser = argparse.ArgumentParser(description='data load')
    parser.add_argument('-u', '--universe', help='universe load date ex. 2017-01-01 or symbol ex. ''ETH-BTC-BTRX''', default=time.strftime("%Y%m%d"), type=str)
    parser.add_argument('-d', '--day', help='day of universe, default: 30', default=30, type=int)
    parser.add_argument('-t', '--top', help='top x of universe, default: 50', default=50, type=int)
    parser.add_argument('-s', '--start', help='start date of data load ex. 2017-01-01 00:00:00', default='2017-11-01 00:00:00', type=str)
    parser.add_argument('-e', '--end', help='end date of data loadnot  ex. 2017-01-01 00:00:00, default : current time', default=time.strftime("%Y%m%dT%H%M%S"), type=str)
    parser.add_argument('-x', '--exchange', help='list of exchages ex. ''BTRX,KRKN'']', default='BTRX,KRKN,BINA,GDAX', type=str)
    parser.add_argument('-p', '--path', help='file directory of data ex. ''/home/ubuntu/AltSim/2.0/data/ohlcv/''', default='', type=str)
    parser.add_argument('-c', '--iscsv', help='True: csv, False: DB', default='True', type=str)

    args = parser.parse_args()

    from_univere = True
    try:
        universe_time = pd.Timestamp(args.universe)
    except:
        symbol = args.universe
        from_univere = False

    d = args.day
    top = args.top
    start_time = pd.Timestamp(args.start)
    end_time = pd.Timestamp(args.end)
    ex = (args.exchange).split(',')
    path = args.path
    is_csv = args.iscsv in ['true', 'True']
    # universe_time = pd.Timestamp('2018-03-30T00:00:00.0000000Z')
    # start_time = pd.Timestamp('2017-11-01T00:00:00.0000000Z')
    # end_time = pd.Timestamp('2018-03-20T00:05:00.0000000Z')
    # end_time = pd.to_datetime(time.strftime("%Y%m%dT%H%M%S"))

    if from_univere:
        uni_sym = get_universe(is_csv, d, top, universe_time, ex)

        for sym in uni_sym:
            has_more_data = True
            while has_more_data:
                base, quote, exchange = sym.split('-')
                marketName = '_'.join(['SPOT', base, quote])
                has_more_data = get_and_store_ohlcv_data(is_csv, exchanges_dict[exchange], marketName, exchange, 'OHLCV_' + marketName, 'aws_exchanges', start_time, end_time, 50000, coinapiKey)
        # print(exchanges_dict[exchange], marketName, exchange,)
        # get_and_store_ohlcv_data(False, exchanges_dict[exchange], marketName, exchange, 'OHLCV_' + marketName, 'aws_exchanges', start_time, end_time, 2, coinapiKey)
    else:
        has_more_data = True
        while has_more_data:
            base, quote, exchange = symbol.split('-')
            marketName = '_'.join(['SPOT', base, quote])
            has_more_data = get_and_store_ohlcv_data(is_csv, exchanges_dict[exchange], marketName, exchange, 'OHLCV_' + marketName, 'aws_exchanges', start_time, end_time, 50000, coinapiKey)

    # skeleton = 'https://rest.coinapi.io/v1/exchanges'
    # get_and_store_ohlcv_data(True, exchanges_dict['BTRX'], 'SPOT_ETH_BTC', 'BTRX', 'OHLCV_' + 'ETH_BTC', 'aws_exchanges', start_time, end_time, 2, coinapiKey)
    # df = pd.DataFrame(requests.get(skeleton, headers=

    # get_last_time_csv('ETH', 'BTC', 'BTRX')
# data.to_csv('uni.csv')
