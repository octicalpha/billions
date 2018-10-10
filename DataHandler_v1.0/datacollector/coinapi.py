import utils.postgresConnection as pgConn
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os, sys
import requests
import datetime
from datetime import datetime, timedelta
import time
import argparse

'''
Set constants:

'''
exchanges_dict = {'BTRX': 'BITTREX',
                  'KBIT': 'KORBIT',
                  'GDAX': 'COINBASE',
                  'BTHM': 'BITHUMB',
                  'CONE': 'COINONE',
                  'GMNI': 'GEMINI',
                  'KRKN': 'KRAKEN',
                  'PLNX': 'POLONIEX',
                  'BINA': 'BINANCE'}
skeleton = 'https://rest-us-east-altcoinadvisors.coinapi.io/v1/ohlcv/{}/history?period_id=1MIN&time_start={}&time_end={}&limit={}'
load_dotenv(find_dotenv())
coinapiKey = 'CD7C1F44-91CD-4223-B857-33A25A8D3D9A' #os.environ.get('COINAPI_KEY_TEMP')
headers = {'X-CoinAPI-Key': coinapiKey}

class DataModule(object):
    def __init__(self,parse_imp,refresh,path):
        self.parse_imp = parse_imp
        self.refresh = refresh
        self.path = path
        '''
        Set class init variables to define specific universe to use
        '''
        self.include_bool = False
        self.include_base = ['BTC','BCH','ETH','ETC','ADA','NEO','XLM','XRP','EOS','LTC','ONT','TRX','VET','IOTA','ICX','QTUM']
        self.include_quote = ['USDT']
        self.exclude_bool = True
        self.exclude_base = ['BLOCK', 'BNT', 'BTCD', 'ENG', 'FCT', 'GBYTE', 'GNO', 'MAID', 'MANA', 'PART', 'REP']
        self.exclude_quote = ['BNB','ETH', 'CAD', 'EUR', 'KRW', 'JPY']
        self.set_exch = ['BINA']

    def get_data(self,exchangeSymbol, marketName, schemaName, tableName, startTime, endTime, limit, coinapiKey):
        existing = False
        try:
            _, base, quote = marketName.split('_')
            subpath = self.path + schemaName + '/'
            if not (os.path.isdir(subpath)):
                os.makedirs(subpath)
            filepath = self.path + '{}/{}-OHLCV_SPOT_{}_{}.csv'
            filename = filepath.format(schemaName, schemaName, base, quote)
            if os.path.isfile(filename):
                try:
                    ohlcv_data = pd.read_csv(filename, index_col=0)
                except:
                    ohlcv_data = pd.DataFrame()
            else:
                ohlcv_data = pd.DataFrame()
            tempTime = startTime
            try:
                startTime = self.get_last_time_csv(ohlcv_data['time'])
                existing = True
            except:
                startTime = tempTime
                existing = False
            print(schemaName, tableName, startTime)
            if startTime >= endTime:
                return False
            # get_coinapi_data
            #import pdb; pdb.set_trace()
            data = self.get_coinapi_data(exchangeSymbol, marketName, startTime, endTime, limit, coinapiKey)
            if data is None:
                return False
            self.update_save_data(ohlcv_data,data,existing,filename)
            return limit == len(data)
        except Exception as e:
            print(e)

    def update_save_data(self,ohlcv_data,data,existing,filename):
        data = self.reformat_data(data)
        data['in_z'] = pd.to_datetime(time.strftime("%Y%m%dT%H%M%S"))
        if existing:
            ohlcv_data = ohlcv_data.append(data, ignore_index=True)
            ohlcv_data.to_csv(filename)
        else:
            data.to_csv(filename)

    def get_coinapi_data(self,exchangeName, marketName, startTime, endTime, limit, coinapiKey):
        marketSym = '_'.join([exchangeName, marketName])
        startTime = datetime.strftime(startTime, '%Y-%m-%dT%H:%M:%SZ')
        endTime = datetime.strftime(endTime, '%Y-%m-%dT%H:%M:%SZ')
        #import pdb; pdb.set_trace()
        try:
            res = requests.get(skeleton.format(marketSym, startTime, endTime, limit), headers=headers)
            print(res.headers['X-RateLimit-Limit'] + '/' + res.headers['X-RateLimit-Remaining'] + '/' + res.headers['X-RateLimit-Request-Cost'])
            if int(res.headers['X-RateLimit-Request-Cost']) > 0:
                df = pd.DataFrame(res.json())
                df['time_period_start'] = pd.to_datetime(df['time_period_start'])
            else:
                return None
        except Exception as e:
            print(marketSym + e)
        return df

    def get_last_time_csv(self,df):
        if len(df) > 0:
            date = df.values[len(df) - 1]
        else:
            return None
        if date is not None:
            date = pd.Timestamp(date)
            date += pd.Timedelta(minutes=1)
        return date

    def reformat_data(self,data):
        oldColsToUse = ['time_period_start', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_traded', 'trades_count']
        newColNames = ['time', 'open', 'high', 'low', 'close', 'baseVolume', 'tradeCount']
        data = data[oldColsToUse]
        nameMapping = {}
        for i in range(len(oldColsToUse)):
            nameMapping[oldColsToUse[i]] = newColNames[i]
        data = data.rename(columns=nameMapping)
        return data

    def run(self,uni):
        """
        def run()
            Class DataCollector will run this function to retrieve data.
            If any parameters required, use parse_imp to import from DataCollector, else ignore.
            The function will return a dataframe or list which will have the data collected in this module.
        """
        self.parse_imp.add_argument('-s', '--start', help='start date of data load ex. 2017-01-01 00:00:00', default='2017-11-01 00:00:00', type=str)
        self.parse_imp.add_argument('-e', '--end', help='end date of data loadnot  ex. 2017-01-01 00:00:00, default : current time', default=time.strftime("%Y%m%dT%H%M%S"), type=str)
        arg_index = sys.argv.index('MODULE')
        args = self.parse_imp.parse_args(sys.argv[arg_index+1:])
        start_time = pd.Timestamp(args.start)
        end_time = pd.Timestamp(args.end)
        data = []
        for sym in uni:
            has_more_data = True
            while has_more_data:
                base, quote, exchange = sym.split('-')
                marketName = '_'.join(['SPOT', base, quote])
                has_more_data = self.get_data(exchanges_dict[exchange], marketName, exchange, 'OHLCV_' + marketName, start_time, end_time, 50000, coinapiKey)
                #import pdb; pdb.set_trace()
