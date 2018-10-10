import utils.postgresConnection as pgConn
import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv
import os, sys
import requests
import datetime
from datetime import datetime, timedelta
import time
import argparse

class DataModule(object):
    def __init__(self,parse_imp,path,env):
        self.parse_imp = parse_imp
        self.path = path
        self.env = env
        '''
        Set class init variables to define specific universe to use
        '''
        self.include_bool = True
        self.include_base = ['BTC','BCH','ETH','ETC','ADA','NEO','XLM']
        self.include_quote = ['USDT']
        self.exclude_bool = False
        self.exclude_base = ['BLOCK', 'BNT', 'BTCD', 'ENG', 'FCT', 'GBYTE', 'GNO', 'MAID', 'MANA', 'PART', 'REP']
        self.exclude_quote = ['BTC','BNB','ETH', 'CAD', 'EUR', 'KRW', 'JPY']
        self.set_exch = ['BINA']

    def get_last_time_db(self,schemaName, tableName, startTime, environment):
        if (pgConn.table_exits(schemaName, tableName, environment)):
            maxTime = pgConn.query('select max(time) from "{}"."{}"'.format(schemaName, tableName), environment=environment, dataframe=True)['max'][0]
            if (maxTime is not None):
                #maxTime += pd.Timedelta(minutes=1)
                return maxTime
        return startTime

    def get_last_time_csv(self,df):
        if len(df) > 0:
            date = df.values[len(df) - 1]
        else:
            return None
        if date is not None:
            date = pd.Timestamp(date)
        return date

    def get_first_time_csv(self,df):
        #if len(df) > 0:
        date = df.values[0]
        #else:
        #    return None
        return date

    def load_data(self,uni,data_dir,interval,startdate=None):
        """
        ftn to import data from a local directory
        """
        dataname = 'ohlcv'
        for col in uni:
            output = pd.DataFrame()
            tick = col.split("-")
            exch = tick[2]
            tablename = dataname.upper() + "_SPOT_" + tick[0] + "_" + tick[1] + "_" + str(interval) + "M"
            csv_dir = data_dir + exch + "/" + exch + "-" + dataname.upper() + "_SPOT_" + tick[0] + "_" + tick[1] + ".csv"
            csv_path = data_dir + exch + "/"
            if not (os.path.isdir(csv_path)):
                os.makedirs(csv_path)
            if os.path.isfile(csv_dir):
                raw_data = pd.read_csv(csv_dir, index_col=0)
                try:
                    enddate = self.get_last_time_csv(raw_data['time'])
                except:
                    enddate = None
            else:
                raw_data = pd.DataFrame()
                try:
                    enddate = self.get_last_time_csv(raw_data['time'])
                except:
                    enddate = None
            startdate = self.get_last_time_db(exch,tablename,startdate,self.env)
            if startdate is None:
                startdate = self.get_first_time_csv(raw_data['time'])
            startdate = pd.Timestamp(startdate)
            startdate = startdate - timedelta(minutes=startdate.minute % interval)
            enddate = pd.Timestamp(enddate)
            if (startdate < enddate):
                print '[DM] Uploading {} from {} to {} ...'.format(exch + "." + tablename,str(pd.to_datetime(startdate)),str(pd.to_datetime(enddate)))
                try:
                    #raw_data['ticker'] = col
                    raw_data['time'] = pd.to_datetime(raw_data['time'])
                    raw_data = raw_data.set_index(['time'])
                    raw_data['time'] = raw_data.index
                    mask = (raw_data['time'] >= str(pd.to_datetime(startdate)-timedelta(hours=1)))
                    subdata = raw_data.loc[mask]
                    interval_min = str(1) + 'min'
                    idx = pd.date_range(pd.to_datetime(startdate)-timedelta(hours=1),pd.to_datetime(enddate),freq=interval_min)
                    subdata = subdata.reindex(idx,fill_value=np.nan)
                    subdata = self.recalc_hl(subdata,interval,startdate,enddate)
                    subdata = subdata.fillna(method='ffill')
                    subdata['time'] = pd.to_datetime(subdata.index)
                    interval_min = str(interval) + 'min'
                    days_range = pd.date_range(pd.to_datetime(startdate), pd.to_datetime(enddate), freq=interval_min)
                    subdata = subdata[subdata['time'].isin(days_range)]
                    #subdata = subdata.set_index(['time'])
                    subdata = subdata.fillna(method='ffill')
                    subdata = subdata.fillna(method='bfill')
                    #import pdb; pdb.set_trace()
                    #subdata['time'] = str(subdata['time'])
                    pgConn.storeInDb(subdata, tablename, self.env, schema=exch)
                except Exception as e:
                    print 'ERROR: {}'.format(exch + '_' + tablename)
                    print(e)

    def recalc_hl(self,data,n,startdate,enddate):
        interval_min = str(n) + 'min'
        idx = pd.date_range(pd.to_datetime(startdate),pd.to_datetime(enddate),freq=interval_min)
        #import pdb; pdb.set_trace()
        for i in idx:
            i_start = pd.to_datetime(i) - timedelta(minutes=n)
            mask = (data['time'] > str(i_start)) & (data['time'] <= str(i))
            data_temp = data.loc[mask]
            data_temp = data_temp.fillna(0)
            #print i_start, i
            try:
                data.loc[i,'open'] = data_temp['open'][0]
                data.loc[i,'high'] = data_temp['high'].max()
                data.loc[i,'low'] = data_temp['low'].min()
                data.loc[i,'close'] = data_temp['close'][-1]
                data.loc[i,'baseVolume'] = data_temp['baseVolume'].sum()
                data.loc[i,'tradeCount'] = data_temp['tradeCount'].sum()
            except:
                pass
        return data

    def run(self,uni):
        """
        def run()
            Class DataManager will run this function to retrieve data.
            If any parameters required, use parse_imp to import from DataManager, else ignore.
            The function will return a dataframe or list which will have the data collected in this module.
        """
        self.parse_imp.add_argument('--min', help='Frequency in minutes', default=5, type=int)
        arg_index = sys.argv.index('MODULE')
        args = self.parse_imp.parse_args(sys.argv[arg_index+1:])
        minutes = args.min
        
        self.load_data(uni,self.path,minutes)
