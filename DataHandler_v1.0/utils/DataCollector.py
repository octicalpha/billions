import pandas as pd
import numpy as np
import argparse
import imp
import os, sys
import utils.postgresConnection as pgConn
from datetime import  *
from datacollector import *

class DataCollector(object):
    def __init__(self,module_class):
        self.mod_class = module_class
    
    def load(self,uni):
        self.mod_class.run(uni)

    def get_universe(self, d, top, uni_time, exchange_list, exclude_bool, exclude_base, exclude_quote, include_bool, include_base, include_quote):
        sql = "select * from universe.mktcap_d{}_top{} where time <= '{}' order by time desc fetch first 5 rows only;"
        try:
            uni_time = datetime.strftime(uni_time, '%Y-%m-%d %H:%M:%S')
            data = pgConn.query(sql.format(d, top, uni_time), environment="aws_dev", dataframe=True)
        except:
            print('ERROR: Check universe')
        universe_symbol_all = []
        length_d = 5
        for i, symbol in enumerate(data.columns):
            tf = [data.values[j][i] for j in range(5)]
            if 'true' in tf:
                base, quote, exchange = symbol.split('-')
                if exclude_bool:
                    if (exchange in exchange_list) and (base not in exclude_base) and (quote not in exclude_quote):
                        universe_symbol_all.append(data.columns[i])
                if include_bool:
                    if (exchange in exchange_list) and (base in include_base) and (quote in include_quote):
                        universe_symbol_all.append(data.columns[i])        
        universe_symbol = list(set(universe_symbol_all))
        return universe_symbol

    def get_universe_local(self, d, top, uni_time, exchange_list, exclude_bool, exclude_base, exclude_quote, include_bool, include_base, include_quote, data):
        universe_symbol_all = []
        length_d = 5
        for i, symbol in enumerate(data.columns):  
            tf = [data.values[j][i] for j in range(5)]
            if True in tf:
                base, quote, exchange = symbol.split('-')
                if exclude_bool:
                    if (exchange in exchange_list) and (base not in exclude_base) and (quote not in exclude_quote):
                        universe_symbol_all.append(data.columns[i])
                if include_bool:
                    if (exchange in exchange_list) and (base in include_base) and (quote in include_quote):
                        universe_symbol_all.append(data.columns[i])    
        universe_symbol = list(set(universe_symbol_all))
        return universe_symbol

def run():
    parser = argparse.ArgumentParser(description='DataCollector : Import data to store locally')
    # Args for DataCollector
    parser.add_argument('--dir', help='Dir of module', type=str)
    parser.add_argument('--refresh', help='Seconds for sleep refresh', type=int, default='5')
    parser.add_argument('-p', '--path', help='File dir of data ex. ''/home/ubuntu/AltSim/2.0/data/ohlcv/''', default='', type=str)
    # Universe
    parser.add_argument('--u_bool', help='Use universes?', default='True', type=str)
    parser.add_argument('--time', help='universe load date ex. 2017-01-01', default='2018-09-01', type=str)
    parser.add_argument('-d', '--day', help='day of universe, default: 30', default=30, type=int)
    parser.add_argument('-t', '--top', help='top x of universe, default: 50', default=50, type=int)
    # If no universe, set individual coins
    parser.add_argument('-l', '--list', help='list of coins ex. ''BTC-USD-KRKN'']', default='BTC-USD-KRKN', type=str)
    arg_index = sys.argv.index('MODULE')
    args = parser.parse_args(sys.argv[1:arg_index])
    mod_dir = args.dir
    mod_refresh = args.refresh
    mod_path = args.path

    u_bool = args.u_bool in ['true','True']
    uni_time = pd.Timestamp(args.time)
    d = args.day
    top = args.top
    clist = (args.list).split(',')
    
    if not (os.path.isdir(mod_path)):
        os.makedirs(mod_path)

    module_class = imp.load_source('',mod_dir)
    DM = module_class.DataModule(parser,mod_refresh,mod_path)
    DC = DataCollector(DM)
    if u_bool:
        uni = DC.get_universe(d, top, uni_time, DM.set_exch, DM.exclude_bool, DM.exclude_base, DM.exclude_quote, DM.include_bool, DM.include_base, DM.include_quote)
    else:
        file_dir = "/home/ubuntu/Afflus_Data/universes/data/universe-mktcap_d{}_top{}.csv"
        file_dir = file_dir.format(d, top)
        data = pd.read_csv(file_dir, index_col=0)
        uni = DC.get_universe_local(d, top, uni_time, DM.set_exch, DM.exclude_bool, DM.exclude_base, DM.exclude_quote, DM.include_bool, DM.include_base, DM.include_quote, data)
    DC.load(uni)

if __name__ == '__main__':
    run()