"""
Optimizer Template
"""

import numpy as np
import pandas as pd
from stats_new import *
from datetime import  *
from util.apply import *
from util.neutralization import Neutralization
from build_core import UniHandler, AlphaHandler
from data.modules.ohlcv import DataHandler
from optimizer.opt_jaekim01 import get_weights

class AlphaMain:
    def __init__(self, startdate, enddate, booksize, download, altsim_dir,pnl_dir):
        self.startdate = startdate
        self.enddate = enddate
        self.download = download
        self.set_exch = ['BINA','KRKN']
        self.set_base = []
        self.set_exclude = ['XRB'] #['BLOCK','BNT','BTCD','ENG','FCT','GBYTE','GNO','MAID','MANA','PART','REP','DOGE','EMC','ARK','RDD','ARDR','KMD','DGB','DCR','LRC','MONA','XZC','BAT','GNT','POWR','PIVX','SC','SYS','CVC','EMC2','GAME','IGNIS','MCO','NXS','PAY','VTC','WAX','ZCL']
        self.set_short_case = ['BTC-USD-KRKN','ETH-USD-KRKN','ETC-USD-KRKN','BCH-USD-KRKN','XRP-USD-KRKN']
        self.set_quote = ['BTC','USD']
        self.opt_alphas_pnl = ['/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim001-bina01_pnl.csv'
                        ,'/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim002-bina01_pnl.csv'
                        ,'/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim003-bina01_pnl.csv'
                        ,'/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim004-bina01_pnl.csv']
        self.opt_alphas_pos = ['/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim001-bina01_pos.csv'
                        ,'/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim002-bina01_pos.csv'
                        ,'/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim003-bina01_pos.csv'
                        ,'/home/ubuntu/AltSim-Execution/2.0/pnl/jaekim004-bina01_pos.csv']
        self.opt_rebalance = ['00:00:00']
        self.opt_backdays = 288
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
                    #import pdb; pdb.set_trace()
                    x.loc[i] = x.loc[i] * btc.loc[i]

        return uni, alpha, ohlcv, ohlcv_close

    def generate(self):
        """
        :param alpha: set of weights
        :param data_index: index where to get data from
        :return: normalized weights
        Main Model Code
        """

        output_alpha = get_weights(self.opt_alphas_pnl,self.opt_alphas_pos,self.alpha,self.opt_rebalance,self.opt_backdays)

        #import pdb; pdb.set_trace()
        self.alpha = output_alpha
        return self.operations(self.alpha)

    def operations(self,alpha_raw):
        output = alpha_raw
        output = self.decay(output,288)
        #output = self.rank(output)
        #neut = Neutralization(output)
        #output = neut.all_neutralize()
        #output = apply_shift_to_pos(output)
        #output = apply_norm(output,long_only=self.longonly,fill_na=False)
        #output = apply_shift_per_side_exch(output,'BTRX','KRKN',self.booksize/2.0)
        output = self.risk_adj(output,'BTC-USD-KRKN',288,1440,0.05,5)
        #output = self.decay(output,288)
        output = apply_scale_to_book(output,self.booksize,self.bookfloat,self.longonly)
        output = apply_short_only(output,'KRKN')
        output = apply_exch_booksize_trunc(output,'BINA','KRKN',0.5,self.booksize)
        output = apply_truncate(output,self.booksize,0.25) #288
        output = self.hump_size(output,20)
        #output = apply_delay(output,1)
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
                elif std_diff.loc[index] > 0.0005 and std_diff.loc[index] > 0.001:
                    row *= 0.8
                elif std_diff.loc[index] > 0.001:
                    row *= 0.7
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
                    row *= 0.75
                output.loc[index] = row

        return output

    def ret(self, col, n):
        """
        calculate returns
        """
        x = self.ohlcv[self.ohlcv['ticker'].str.contains(col)]
        x_1 = x.shift(n)
        #import pdb; pdb.set_trace()
        return (x['close'] - x_1['close']) / x_1['close'] #(ohlcv_filter[field] - x_bar[field]) / std[field]

    def rank(self, data):
        """
        rank values [0,1]
        """
        min_val = data.min(axis=1)
        max_val = data.max(axis=1)
        temp1 = data.subtract(min_val,axis='index')
        temp2 = max_val.subtract(min_val,axis='index')
        return temp1.divide(temp2,axis='index')

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

    def hump_size(self, data, initsize):
        """
        value changes if value is over a certain % change
        """
        diff = data.diff(axis=0)
        output = data
        for col in data:
            for i in data.index:
                if abs(diff.loc[i][col]) < initsize:
                    #import pdb; pdb.set_trace()
                    i_prev = i - timedelta(minutes=self.interval)
                    output.loc[i][col] = output.loc[i_prev][col]
        return output

    def short_match_with_ticker(self, data, ticker):
        """
        matches long/short for btc short
        """
        output = data
        sum_data = data.sum(axis=1)
        output[ticker] = -1.0 * sum_data
        return output
