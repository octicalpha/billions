"""
Main file that people will use to create models
"""

import numpy as np
import pandas as pd
from util.apply import *
from util.neutralization import Neutralization
from build_core import UniHandler, AlphaHandler
from data.modules.ohlcv import DataHandler
from stats_new import *
from datetime import  *

class AlphaMain:
    def __init__(self, startdate, enddate, booksize, download, altsim_dir,pnl_dir):
        self.startdate = startdate
        self.enddate = enddate
        self.download = download
        self.set_exch = ['BINA','KRKN']
        self.set_base = [] #['BTC','XRP','ETH','XVG','BCH','LTC','NEO','ADA','OMG','ETC','ZEC','BTG','NXT','XMR','DASH'] # avoid using unless data is scarse 
        self.set_exclude = ['XRB'] #['BNB','NEO','QTUM'] #['BLOCK','BNT','BTCD','ENG','FCT','GBYTE','GNO','MAID','MANA','PART','REP','DOGE','EMC','ARK','RDD','ARDR','KMD','DGB','DCR','LRC','MONA','XZC','BAT','GNT','POWR','PIVX','SC','SYS','CVC','EMC2','GAME','IGNIS','MCO','NXS','PAY','VTC','WAX','ZCL']
        self.set_short_case = ['BTC-USD-KRKN','ETH-USD-KRKN','ETC-USD-KRKN','BCH-USD-KRKN','XRP-USD-KRKN'] #['BTC-USD-KRKN','ETH-USD-KRKN','ETC-USD-KRKN','BCH-USD-KRKN','XRP-USD-KRKN','ZEC-USD-KRKN','XMR-USD-KRKN']
        self.set_quote = ['BTC','USD']
        self.uni_name = 'mktcap_d30_top50'
        self.backdays = 5 # Defined as full days
        self.interval = 5 # Defined as minutes
        self.booksize = booksize
        self.bookfloat = False
        self.longonly = False
        self.altsim_dir = altsim_dir
        self.pnl_dir = pnl_dir
        self.uni, self.alpha, self.ohlcv, self.ohlcv_close = self.initializes()

    def initializes(self):
        """ 
        initialize the data to be used within generate
        """
        # Universe
        UH = UniHandler(self.startdate,self.enddate,True,self.altsim_dir)
        uni = UH.build_uni(self.uni_name,'aws_dev')
        uni = UH.filter_uni(uni,self.set_exch,self.set_base,self.set_quote,self.set_exclude,self.set_short_case)
        uni['BTC-USDT-BINA'] = True
        # Data
        DH = DataHandler(self.startdate,self.enddate,self.backdays,self.interval,self.download,self.altsim_dir)
        # include data you will use
        ohlcv = DH.build_data(uni,'ohlcv','aws_exchanges')
        ohlcv_close = apply_to_matrix(ohlcv,'close','ticker',self.startdate,self.enddate,self.interval)
        ohlcv_close = backfill(ohlcv_close)

        # Remove tickers that dont have data
        uni = apply_filter_no_data_tickers(uni, ohlcv_close)

        # Alpha
        AH = AlphaHandler(self.startdate,self.enddate,self.interval)
        alpha = AH.build_df(uni)
        del alpha['BTC-USDT-BINA']

        # Recalculate */BTC to */USDT for BINA
        for col in alpha:
            if col.split('-')[2] == 'BINA':
                x = ohlcv_close.loc[:,col]
                btc = ohlcv_close.loc[:,'BTC-USDT-BINA']
                for i in x.index:
                    x.loc[i] = x.loc[i] * btc.loc[i]

        return uni, alpha, ohlcv, ohlcv_close

    def generate(self):
        """
        :param alpha: set of weights
        :param data_index: index where to get data from
        :return: normalized weights
        Main Model Code
        """

        for col in self.alpha:
            z = self.z_score_ret_mom(col,1,36,288*5,'close')
            weights = apply_date_filter(z,self.startdate,self.enddate)
            self.alpha[col] = weights
        self.alpha = self.alpha.fillna(0)
        return self.operations(self.alpha)

    def operations(self,alpha_raw):
        output = alpha_raw
        output = self.decay(output,288*5) #288
        neut = Neutralization(output)
        output = neut.all_neutralize()
        output = apply_scale_to_book(output,self.booksize,self.bookfloat,self.longonly)
        output = apply_shift_per_side_exch(output,'BINA','KRKN',self.booksize/2.0)
        #output = self.decay(output,288*5) #288
        output = apply_truncate(output,self.booksize,0.25) #288
        output = apply_delay(output,1)
        return output

    def z_score(self, col, n, field):
        """
        gets zscore of data
        """
        ohlcv_filter = self.ohlcv[self.ohlcv['ticker'].str.contains(col)]
        x = ohlcv_filter.rolling(6,axis=0,min_periods=1).mean()
        x_bar = ohlcv_filter.rolling(n,axis=0,min_periods=1).mean()
        std = ohlcv_filter.rolling(n,axis=0,min_periods=1).std()

        return (x[field] - x_bar[field]) / std[field] #(ohlcv_filter[field] - x_bar[field]) / std[field]

    def z_score_ret_mom(self, col, n_ret, n1, n2, field):
        """
        gets zscore of data
        """
        ohlcv_filter = self.ohlcv[self.ohlcv['ticker'].str.contains(col)]
        ret = self.ret(col,n_ret)
        x1 = ret.rolling(n1,axis=0,min_periods=1).mean()
        x2 = ret.rolling(n2,axis=0,min_periods=1).mean()
        std1 = ret.rolling(n1,axis=0,min_periods=1).std()
        std2 = ret.rolling(n2,axis=0,min_periods=1).std()

        return (x1 / std1) - (x2 / std2) 

    def ret(self, col, n):
        """
        calculate returns
        """
        x = self.ohlcv_close.loc[:,col]
        #if col.split('-')[2] == 'BTRX':
        #    btc = self.ohlcv_close.loc[:,'BTC-USDT-BTRX']
        #    for i in x.index:
                #import pdb; pdb.set_trace()
        #        x.loc[i] = x.loc[i] * btc.loc[i]
        x_1 = x.shift(n)
        return (x - x_1) / x_1 #(ohlcv_filter[field] - x_bar[field]) / std[field]

    def booksize_adj(self,alpha,col,n1,n2):
        """
        TODO: make it to adj for mkt std
        """
        output = alpha
        ohlcv_filter = self.ohlcv[self.ohlcv['ticker'].str.contains(col)]
        ret_mkt = self.ret(col,1)
        std1 = ret_mkt.rolling(n1,axis=0,min_periods=1).std()
        std2 = ret_mkt.rolling(n2,axis=0,min_periods=1).std()
        std_diff = std2 - std1
        #std_diff = std_diff.fillna(1)
        #std_diff.replace(np.inf, 1, inplace=True)
        for i,row in output.iterrows():
            try:
                if std_diff.loc[i] > 0.00025 and std_diff.loc[i] < 0.0005:
                    output.loc[i] *= 0.9
                elif std_diff.loc[i] > 0.0005 and std_diff.loc[i] < 0.001:
                    output.loc[i] *= 0.8
                elif std_diff.loc[i] > 0.001:
                    output.loc[i] *= 0.75
            except:
                pass
        return output

    def dd_liquidate(self, alpha, bound, n):
        """
        WARNING: Only run after you have generated initial pnl file without this ftn
        """
        output = alpha
        #alpha_pnl = pd.read_csv(self.pnl_dir,index_col=0)
        for index, row1 in output.iterrows():
            index_start = pd.to_datetime(index - timedelta(n))
            if index_start < pd.to_datetime(self.startdate):
                index_start = pd.to_datetime(self.startdate)
            if index == index_start:
                continue
            else:
                pos_temp = output.loc[index_start:index]
                prices = self.ohlcv_close.loc[index_start:index]
                prevRow, prevPrice = pos_temp.iloc[0], prices.iloc[0]
                cumPnL = 0
                for i, row2 in pos_temp.iterrows():
                    currPrice = prices.loc[index]
                    currRow = row2
                    intPnL = sum(PnL(currRow, currPrice, prevRow, prevPrice))
                    cumPnL += intPnL
                pnl_book_ratio = cumPnL / self.booksize            
                if pnl_book_ratio < -1.0 * bound:
                    row1 *= 0.5
                output.loc[index] = row1
        #import pdb; pdb.set_trace()        
        return output

    def risk_adj(self,alpha,col,n1,n2,bound,n):
        """
        TODO: make it to adj for mkt std
        """
        output = alpha
        ohlcv_filter = self.ohlcv[self.ohlcv['ticker'].str.contains(col)]
        ret_mkt = self.ret(col,1)
        std1 = ret_mkt.rolling(n1,axis=0,min_periods=1).std()
        std2 = ret_mkt.rolling(n2,axis=0,min_periods=1).std()
        std_diff = std2 - std1

        cumPnL = 0
        pos_temp = output
        prices = self.ohlcv_close
        prevRow, prevPrice = pos_temp.iloc[0], prices.iloc[0]
        pnl_temp = pd.DataFrame(0.0,columns=['PnL'],index=output.index)
        for index,row in output.iterrows():
            currPrice = prices.loc[index]
            currRow = row
            pnl_temp.loc[index]['PnL'] = sum(PnL(currRow, currPrice, prevRow, prevPrice))
            prevRow = currRow
            prevPrice = currPrice
        for index,row in output.iterrows():
            # Booksize Risk Adjustment
            try:
                if std_diff.loc[index] > 0.00025 and std_diff.loc[index] < 0.0005:
                    row *= 0.9
                elif std_diff.loc[index] < 0.0005 and std_diff.loc[index] < 0.001:
                    row *= 0.8
                elif std_diff.loc[index] > 0.001:
                    row *= 0.75
            except:
                pass

            # Drawdown Risk Adjustment
            index_start = pd.to_datetime(index - timedelta(n))
            if index_start < pd.to_datetime(self.startdate):
                index_start = pd.to_datetime(self.startdate)
            if index == index_start:
                continue
            else:
                pnl_book_ratio = pnl_temp.loc[index_start:index]['PnL'].sum() / self.booksize
                if pnl_book_ratio < -1.0 * bound:
                    row *= 0.25
                output.loc[index] = row

        return output

    def pump(self, col, percent, n):
        """
        gets pump signal
        """
        col_split = col.split('-')
        ret_col = self.ret(col,n)
        for index in ret_col.index:
            if col_split[1] in ['BTC'] and col_split[2] in ['BTRX']:
                #if ret_col[index] > percent:
                #    ret_col[index] = 0.0
                if ret_col[index] < (-1.0*percent):
                    ret_col[index] = np.abs(ret_col[index]) - percent
                #else:
                #    ret_col[index] = 0
            elif col_split[1] in ['USD'] and col_split[2] in ['KRKN']:
                if ret_col[index] > percent:
                    ret_col[index] = -1.0 * (np.abs(ret_col[index]) - percent)
                #else:
                #    ret_col[index] = 0
                #if ret_col[index] < (-1*percent):
                #    ret_col[index] = 0.0
        return ret_col

    def decay(self, data, n):
        """
        gets n period decay of data
        """
        return data.rolling(n,axis=0,min_periods=1).mean()

    def hump(self, data, percent, initsize):
        """
        value changes if value is over a certain % change
        """
        diff = data.diff(axis=0)
        output = data
        for col in data:
            for i in data.index:
                if abs(diff[col][i]) < percent*initsize:
                    output[col][i] = output[col][i-1]
        return output

    def short_match_with_ticker(self, data, ticker):
        """
        matches long/short for btc short
        """
        output = data
        sum_data = data.sum(axis=1)
        output[ticker] = -1.0 * sum_data
        return output
