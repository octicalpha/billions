import os,sys
import pandas as pd
import numpy as np
import argparse
import time
from datetime import datetime, timedelta
#from forex_python.converter import CurrencyRates
from utils.postgresConnection import query

def get_data_range(index,startdate,enddate,days,option):
    """ 
    get_data_range: setup query for sql
    """
    schema = 'coincap'
    tablename = 'history'
    start_epoch = str(time.mktime(time.strptime(str(pd.to_datetime(startdate).date()-timedelta(days)),'%Y-%m-%d')))[:-2] + '000'
    end_epoch = str(time.mktime(time.strptime(str(pd.to_datetime(enddate).date()),'%Y-%m-%d')))[:-2] + '000'
    q_data = """select * from {}.{} where symbol='{}' and ts >= '{}' and ts < '{}' order by ts asc""".format(schema,tablename,index,start_epoch,end_epoch)
    data = query(q_data, environment='aws_dev',dataframe=True)
    return data

def get_cumulative_value(uni,data,ticker,startdate,days,option='volume'):
    """ 
    get_cumulative_value: calculate by option
        volume = cumulate volume USD value
    """
    index_date = pd.to_datetime(startdate)
    for col in uni:
        if len(str(col)) == 10:
            col_epoch = str(time.mktime(time.strptime(str(col),'%Y-%m-%d')))[:-2] + '000'
        else:
            col_epoch = str(time.mktime(time.strptime(str(col)[:-9],'%Y-%m-%d')))[:-2] + '000'
        if len(str(startdate)) == 10:
            startcalc_epoch = str(time.mktime(time.strptime(str(startdate),'%Y-%m-%d')))[:-2] + '000'
        else:
            startcalc_epoch = str(time.mktime(time.strptime(str(startdate)[:-9],'%Y-%m-%d')))[:-2] + '000'
        if col_epoch >= startcalc_epoch:
            start_epoch = str(time.mktime(time.strptime(str(index_date-timedelta(days))[:-9],'%Y-%m-%d')))[:-2] + '000'
            end_epoch = str(time.mktime(time.strptime(str(index_date+timedelta(1))[:-9],'%Y-%m-%d')))[:-2] + '000'
            mask = (data['ts'] >= int(start_epoch)) & (data['ts'] < int(end_epoch))
            subdata = data.loc[mask]
            if option == 'volume':
                uni[col][ticker] = subdata['volume'].mean()                
            elif option == 'mktcap':
                uni[col][ticker] = subdata['marketcap'].mean()
            index_date = index_date + timedelta(1)
    return uni

def get_backfill(uni):
    for row in uni.index:
        for col in uni[1:]:
            if uni[col][row] == '' or np.isnan(uni[col][row]):
                prev_col = pd.to_datetime(col).date() - timedelta(1)
                try:
                    uni[col][row] = uni[str(prev_col)][row]
                except:
                    pass
    #import pdb; pdb.set_trace()
    return uni

def get_dynamic_uni(inst,startdate,enddate,days,option='volume'):
    """ 
    get_dynamic_uni: returns new raw values for universe construction
    """
    days_range = pd.date_range(pd.to_datetime(startdate), pd.to_datetime(enddate)-timedelta(1), freq='D')
    ticker_unique = inst.ticker.unique()
    ticker_unique = [i.split('-')[0] for i in ticker_unique]
    ticker_unique = list(set(ticker_unique))
    ticker_unique = [i for i in ticker_unique if i != 'BTC']
    ticker_unique = ['BTC'] + ticker_unique
    uni = pd.DataFrame(index=ticker_unique,columns=days_range)
    for index in uni.index:
        print 'Loading {} from {} to {} ...'.format(index,startdate,enddate)
        data = get_data_range(index,startdate,enddate,days,option)
        uni = get_cumulative_value(uni,data,index,days_range[0],days,option)
    return uni

def update_dynamic_uni(inst,uni,startdate,enddate,days,option='volume'):
    """ 
    update_dynamic_uni: returns updated raw values for universe construction
    """
    ticker_unique = inst.ticker.unique()
    ticker_unique = [i.split('-')[0] for i in ticker_unique]
    ticker_unique = list(set(ticker_unique))
    ticker_unique = [i for i in ticker_unique if i != 'BTC']
    ticker_unique = ['BTC'] + ticker_unique
    days_range = pd.date_range(pd.to_datetime(startdate), pd.to_datetime(enddate)-timedelta(1), freq='D')
    last_date = uni.columns[len(uni.columns)-1]
    mask = (days_range > last_date)
    if sum(mask) > 0 :
        days_range = days_range[mask].date
        uni[days_range] = pd.DataFrame(index=ticker_unique,columns=days_range)
        px = pd.DataFrame(index=ticker_unique,columns=days_range)
        for index in ticker_unique:
            if index in uni.index:
                print 'Updating {} from {} to {} ...'.format(index,days_range[0],days_range[len(days_range)-1])
                data = get_data_range(index,days_range[0],days_range[len(days_range)-1]+timedelta(1),days,option)
                uni = get_cumulative_value(uni,data,index,days_range[0],days,option)
    for index in ticker_unique:
        if index not in uni.index:
            print 'Adding new {} from {} to {} ...'.format(index,uni.columns[0],days_range[len(days_range)-1])
            uni.loc[index] = 'NaN'
            days_range = pd.date_range(pd.to_datetime(uni.columns[0]), pd.to_datetime(days_range[len(days_range)-1]), freq='D')
            px = pd.DataFrame(index=[ticker_unique],columns=days_range)
            data_btc = get_data_range('BTC',uni.columns[0],days_range[len(days_range)-1]+timedelta(1),days,option)
            data = get_data_range(index,uni.columns[0],days_range[len(days_range)-1]+timedelta(1),days,option)
            uni = get_cumulative_value(uni,data,index,days_range[0],days,option)
    return uni

def get_topn_uni(inst,uni,n,days):
    """ 
    get_topn_uni: constructs top n universe
    TODO: smoothen out the universes raw data with MA(days)
    """
    col_temp = ['ticker'] + list(uni.columns.values)
    output_uni = pd.DataFrame(index=inst.index.values,columns=col_temp)
    output_uni['ticker'] = inst['ticker']
    uni = uni.rolling(days+1,axis=1,min_periods=1).mean()
    uni = get_backfill(uni)
    save_filter_uni = uni[uni.columns[0]].nlargest(n)
    for col in uni:
        if uni[col].isnull().sum() < len(uni[col])-1 :
            filter_uni = uni[col].nlargest(n)
            mask = (uni[col] >= filter_uni[len(filter_uni)-1])
            for row in output_uni.index:
                output_uni[col][row] = mask[inst['base'][row]]
            save_filter_uni = filter_uni
        else:
            output_uni[col] = False
    return output_uni

def get_mincut_uni(inst,uni,n,days):
    """ 
    get_mincut_uni: constructs universe based on minimum volume
    TODO: smoothen out the universes raw data with MA(days)
    """
    col_temp = ['ticker'] + list(uni.columns.values)
    output_uni = pd.DataFrame(index=inst.index.values,columns=col_temp)
    output_uni['ticker'] = inst['ticker']
    uni = uni.rolling(days+1,axis=1,min_periods=1).mean()
    uni = get_backfill(uni)
    for col in uni:
        if uni[col].isnull().sum() < len(uni[col])-1 :
            mask = (uni[col] >= n*1000000)
            for row in output_uni.index:
                output_uni[col][row] = mask[inst['base'][row]]
        else:
            output_uni[col] = False
    return output_uni

if __name__ == '__main__':
    """
    How to use:
    1. Create raw values - this is to calculate USD volume of coins. NOTE: hardcoded in static_uni.py small subset of uni to test until we can speed up
        python dynamic_uni.py -s 2017-08-16 -e 2017-12-20
    2. Create universe - create universes with bool
        - TOP N -> creates universe on n largest based, universe will always include these top n coins
        python dynamic_uni.py -s 2017-08-16 -e 2017-12-20 --top True --topn 5 -d 20
        - MINCUT -> creates universe with at least n million USD of volume, universe will be cumulative and theoretically an increasing function
        python dynamic_uni.py -s 2017-08-16 -e 2017-12-20 --mincut True --mincutn 100 -d 20
        
    """
    parser = argparse.ArgumentParser(description='Create raw dynamic universe values and then create universes. Must run static_uni.py before to have map of instruments.')
    #parser.add_argument('--dir', help='folder dir of static_uni.csv', type=str, default='')
    parser.add_argument('-s','--start', help='start date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-e','--end', help='end date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('--path_dyn', help='path', type=str)
    parser.add_argument('-d','--days', help='number of days vwap will be calculated, if --top or --mincut True: MA(days)', type=int, default=1)
    parser.add_argument('-o','--option', help='volume,mktcap', type=str, default='volume')
    parser.add_argument('--output', help='output folder', type=str, default='data/')
    parser.add_argument('--top', help='BOOL: create a TOP N uni?', type=bool, default=False)
    parser.add_argument('--topn', help='how many for TOP N uni?', type=int, default=5)
    parser.add_argument('--mincut', help='BOOL: create a MIN CUT uni?', type=bool, default=False)
    parser.add_argument('--mincutn', help='minimum mn USD size for MIN CUT uni?', type=int, default=100)

    args = parser.parse_args()
    startdate = args.start
    enddate = args.end
    days = args.days - 1
    option = args.option
    output = args.output
    top = args.top
    topn = args.topn
    mincut = args.mincut
    mincutn = args.mincutn
    path_dyn = args.path_dyn

    #import pdb; pdb.set_trace()
    inst_path = path_dyn + output + 'static_uni.csv'
    inst = pd.read_csv(inst_path, index_col=0)

    filename = path_dyn + output + '{}_raw_uni.csv'.format(option)
    if os.path.isfile(filename):
        old_uni = pd.read_csv(filename, index_col=0)
        output_uni = update_dynamic_uni(inst,old_uni,startdate,enddate,days,option)
        os.remove(filename)
        output_uni.to_csv(filename)
    else:
        output_uni = get_dynamic_uni(inst,startdate,enddate,days,option)
        output_uni.to_csv(filename)

    if top:
        topn_uni = get_topn_uni(inst,output_uni,topn,days)
        filename_topn = path_dyn + output + '{}_d{}_top{}_uni.csv'.format(option,days+1,topn)
        if os.path.isfile(filename_topn):
            os.remove(filename_topn)
            topn_uni.to_csv(filename_topn)
        else:
            topn_uni.to_csv(filename_topn)

    if mincut:
        mincut_uni = get_mincut_uni(inst,output_uni,mincutn,days)
        filename_mincut = path_dyn + output + '{}_d{}_mincut{}_uni.csv'.format(option,days+1,mincutn)
        if os.path.isfile(filename_mincut):
            os.remove(filename_mincut)
            mincut_uni.to_csv(filename_mincut)
        else:
            mincut_uni.to_csv(filename_mincut)