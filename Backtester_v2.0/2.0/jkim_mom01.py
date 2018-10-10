"""
Main file that people will use to create models
"""

import numpy as np
import pandas as pd
from util.apply import *
from util.neutralization import Neutralization
from build_core import UniHandler, AlphaHandler
from data.modules.ohlcv import DataHandler

class AlphaMain:
    def __init__(self, startdate, enddate, booksize, download):
        self.startdate = startdate
        self.enddate = enddate
        self.download = download
        #self.set_exch = ['BTRX','GDAX','GMNI','KRKN','BTHM','CONE','KBIT']
        self.set_exch = ['BTRX','GMNI']
        self.set_base = [] #['BTC','XRP','ETH','XVG','BCH','LTC','NEO','ADA','OMG','ETC','ZEC','BTG','NXT','XMR','DASH'] # avoid using unless data is scarse 
        self.set_quote = ['BTC','USD']
        self.uni_name = 'mktcap_d30_top100'
        self.backdays = 5 # Defined as full days
        self.interval = 5 # Defined as minutes
        self.booksize = booksize
        self.bookfloat = False
        self.longonly = False
        self.uni, self.alpha, self.ohlcv, self.ohlcv_close = self.initializes()

    def initializes(self):
        """ 
        initialize the data to be used within generate
        """
        # Universe
        UH = UniHandler(self.startdate,self.enddate,self.download)
        uni = UH.build_uni(self.uni_name,'aws_dev')
        uni = UH.filter_uni(uni,self.set_exch,self.set_base,self.set_quote)
        
        # Data
        DH = DataHandler(self.startdate,self.enddate,self.backdays,self.interval,self.download)
        # include data you will use
        ohlcv = DH.build_data(uni,'ohlcv','aws_exchanges')
        ohlcv_close = apply_to_matrix(ohlcv,'close','ticker',self.startdate,self.enddate)
        ohlcv_close = backfill(ohlcv_close)

        # Remove tickers that dont have data
        uni = apply_filter_no_data_tickers(uni, ohlcv_close)

        # Alpha
        AH = AlphaHandler(self.startdate,self.enddate,self.interval)
        alpha = AH.build_df(uni)
        
        return uni, alpha, ohlcv, ohlcv_close

    def generate(self):
        """
        :param alpha: set of weights
        :param data_index: index where to get data from
        :return: normalized weights
        Main Model Code
        """

        for col in self.alpha:
            col_split = col.split('-')
            if col_split[1] in ['BTC'] and col_split[2] in ['BTRX']:
                z1 = self.z_score(col,72,1440,'close')
                z2 = self.z_score(col,12,288,'close')
                z = z2 - z1
                weights = apply_date_filter(z,self.startdate,self.enddate)
                self.alpha[col] = -1.0 * weights
        return self.operations(self.alpha)

    def operations(self,alpha_raw):
        output = alpha_raw
        output = self.decay(output,700) #288
        neut = Neutralization(output)
        output = neut.all_neutralize()
        output = apply_shift_to_pos(output)
        output = apply_norm(output,long_only=self.longonly,fill_na=False)
        output = apply_scale_to_book(output,self.booksize/2.0,self.bookfloat,self.longonly)
        output = self.short_match_with_ticker(output,'BTC-USD-GMNI')
        #output = self.hump(output,0.00025,self.booksize)
        output = apply_delay(output,1)
        #import pdb; pdb.set_trace()
        return output

    def z_score(self, col, n1, n2, field):
        """
        gets zscore of data
        """
        ohlcv_filter = self.ohlcv[self.ohlcv['ticker'].str.contains(col)]
        x = ohlcv_filter.rolling(n1,axis=0,min_periods=1).mean()
        x_bar = ohlcv_filter.rolling(n2,axis=0,min_periods=1).mean()
        std = ohlcv_filter.rolling(n2,axis=0,min_periods=1).std()

        return (x[field] - x_bar[field]) / std[field] #(ohlcv_filter[field] - x_bar[field]) / std[field]

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
