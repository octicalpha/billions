"""
Main file that people will use to create models
"""
import numpy as np
import pandas as pd
from util.apply import *
from datetime import *
from data.modules.ohlcv_handler import OhlcvHandler
from data.modules.uni_handler import UniHandler
from AlphaBaseClass import AlphaBaseClass

class AlphaMain(AlphaBaseClass):
    def __init__(self, startdate, enddate, booksize):
        self.startdate = startdate
        self.enddate = enddate
        self.booksize = booksize
        self.bookfloat = False
        self.longonly = False

        self.ohlcv_close = None

    def load_data(self, altsim_dir, download):
        exch = ['BINA']
        quote = ['USDT']
        base = ['BTC', 'BCH', 'ETH']
        exclude = []
        short_case = []
        interval = '5T'  # in min
        uni_name = 'mktcap_d30_top50'

        UH = UniHandler(altsim_dir)
        UH.get_data(uni_name, self.startdate, self.enddate, download,
                    exch, base, quote, exclude, short_case)

        DH = OhlcvHandler(altsim_dir)
        tickers = UH.essentialData.columns
        DH.get_data(tickers, self.startdate, self.enddate, interval, download)

        return {'ohlcv':DH,'uni': UH}

    def get_param_dict_for_grid_search(self):
        return {
            'BACK_DAYS': [5, 10],
            'n1': [122],
            'n2': [122],
            'corr_bound': [.7],
            'min_len': [122],
            'min_margin': [.005],
            'tcost': [.001]}

    def get_params_for_single_run(self):
        return {'BACK_DAYS': 5,
         'INTERVAL': 5,
         'n1': 122,
         'n2': 122,
         'corr_bound': .7,
         'min_len': 122,
         'min_margin': .005,
         'tcost': .001}

    def generate(self, dataHandlers, params):
        """
        :param alpha: set of weights
        :param data_index: index where to get data from
        :return: normalized weights
        Main Model Code
        """

        ohlcv_close = dataHandlers['ohlcv'].get_col('close')
        self.ohlcv_close = ohlcv_close
        alpha = pd.DataFrame(0, index=ohlcv_close.index,
                             columns=dataHandlers['uni'].essentialData.columns)
        for col in alpha:
            z = self.pairs(ohlcv_close, col, params['n1'], params['n2'], params['corr_bound'],
                           params['min_len'], params['min_margin'])
            alpha[col] = pd.Series(data=z,index=alpha.index)
        alpha = alpha.fillna(0)

        x = self.operations(alpha, ohlcv_close)
        return x

    def operations(self,alpha_raw, ohlcv_close):
        output = alpha_raw
        output = self.decay(output,12) #288
        #neut = Neutralization(output)
        #output = neut.all_neutralize()
        #output = neut.static_neutralize('base','/home/ubuntu/AltSim/2.0/data/universe/universe-static_uni.csv')
        #output = neut.static_neutralize('exchange','/home/ubuntu/AltSim/2.0/data/universe/universe-static_uni.csv')
        #output = self.zscore(output,1440)
        #output = self.decay(output,6) #288
        output = apply_scale_to_book(output,1.0,self.bookfloat,self.longonly)
        output = apply_truncate(output,self.booksize,0.5) #288
        output = apply_to_coin(output,ohlcv_close,2.0,'BTC-USDT-BINA')
        output = self.flip(output)
        output = self.convert_to_usd(output, ohlcv_close)
        output = self.freq(output,60*6)
        output = self.hump_size(output,35000)
        #output = apply_delay(output,1)
        return output

    def pairs(self, ohlcv_close, col1, n1, n2, corr_bound, min_len, min_margin):
        """
        pairs trading
        """
        output_mod = np.zeros((len(ohlcv_close),))

        ret = ohlcv_close
        ret = ret.fillna(0)
        for col2 in ohlcv_close:
            if col1 != col2:
                x1 = ret.loc[:,col1]
                x2 = ret.loc[:,col2]
                corr = pd.rolling_corr(x1,x2,n1)
                x1_mod = x1.values
                x2_mod = x2.values
                corr_mod = corr.values
                for i in range(0,len(corr)):
                    if np.fabs(corr_mod[i]) > corr_bound and i >= n2:
                        x1_sub_mod = x1_mod[i-n2:i]
                        x2_sub_mod = x2_mod[i-n2:i]
                        x2_sub_mod = np.vstack([x2_sub_mod, np.ones(len(x2_sub_mod))]).T
                        beta, regression_const = np.linalg.lstsq(x2_sub_mod, x1_sub_mod)[0]

                        try:
                            op = np.sign(corr_mod[i]) * (x1_sub_mod[-1] - (regression_const+beta*x2_sub_mod[-1][0]))

                            margin = np.fabs(op) / x1_sub_mod[-1]

                            if np.fabs(margin) >= np.fabs(min_margin):
                                output_mod[i] += op
                        except:
                            output_mod[i] = output_mod[i-1]
                        #output[col1][i] += np.sign(corr[i]) * (x1_sub.iloc[-1] - (model.params[0]+model.params[1]*x2_sub.iloc[-1][-1]))

        return output_mod

    def zscore(self,alpha,n):
        mean_temp = alpha.rolling(n,axis=0).mean()
        std_temp = alpha.rolling(n,axis=0).std()
        output = (alpha - mean_temp) / std_temp
        return output

    def flip(self,alpha):
        output = -1.0 * alpha
        return output

    def ret_matrix(self, n, ohlcv_close):
        """
        calculate returns
        """
        x = ohlcv_close
        x_temp = x.shift(n)
        ret = (x - x_temp) / x_temp
        return ret

    def convert_to_usd(self, alpha, ohlcv_close):
        """
        changes coin to usd m2m
        """
        output = alpha
        for col in output:
            output[col] = output[col] * ohlcv_close[col]
        return output

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
                    i_prev = i - timedelta(minutes=60*6)
                    output.loc[i][col] = output.loc[i_prev][col]
        return output

    def counter_hump_size(self, data, initsize, interval):
        """
        value changes if value is over a certain % change
        """
        diff = data.diff(axis=0)
        output = data
        for col in data:
            for i in data.index:
                if abs(diff.loc[i][col]) > initsize:
                    #import pdb; pdb.set_trace()
                    i_prev = i - timedelta(minutes=interval)
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

    def freq(self, alpha, freq_min):
        """
        freq to trade
        """
        output = alpha
        interval_min = str(freq_min) + 'min'
        idx = pd.date_range(pd.to_datetime(self.startdate),pd.to_datetime(self.enddate),freq=interval_min)
        output = output.loc[idx]
        for i in output.index:
            if i not in idx:
                output.loc[i] = output.loc[i-1]
        return output
