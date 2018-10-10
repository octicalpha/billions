import time
from datetime import datetime
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import os
import argparse
import matplotlib
from collections import defaultdict

from stats import get_stats

matplotlib.rc('xtick', **{'labelsize': 8})
matplotlib.rc('legend', **{'fontsize': 10})


class Visual(object):

    def __init__(self, interval, start_time, end_time, pos_filepath, tcost,
                 ma, coins):

        self.ma = ma
        self.configure()

        print "[Visual] Initializing."
        self.initialize(interval, start_time, end_time, pos_filepath, tcost,
                        coins)

        print "[Visual] Loading cache data."
        self.check_cache()
        self.recall()
        
        print "[Visual] Loading required data."
        self.memorize()
        
        print "[Visual] Complete."

    def configure(self):

        self.meta = defaultdict(bool, {
            "price_df": {
                "func": "set_price_df", "args": []
            },
            "cumpnl_se": {
                "func": "set_cumpnl_se", "args": []
            },
            "pnl_dict": {
                "func": "set_pnl_dict", "args": [
                    "sort_largest", "sort_most", "sort_least"
                ]
            },
            "ma_df": {
                "func": "set_ma_df", "args": []
            }
        })

        self.whitelist = []
        self.checklist = [
            "global_price_df",
            "price_df",
            "cumpnl_se",
            "pnl_dict",
            "coin_df",
            "ma_df" if self.ma else None,
            "coins4ma" if self.ma else None
        ]

    def initialize(self, interval, start_time, end_time, pos_filepath, tcost,
                   coins):

        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.tcost = tcost
        pos_df = pd.read_csv(pos_filepath, index_col=0)
        pos_df.index = pd.to_datetime(pos_df.index)
        self.pos_df = pos_df
        self.idx_first = self.pos_df.index[0]
        self.idx_last = self.pos_df.index[-1]
        self.min_time_delta = self.pos_df.index[1]-self.idx_first
        self.exchanges = set(name.split("-")[-1]
                             for name in self.pos_df.columns)
        self.booksize = round(sum(self.pos_df.iloc[0].abs()), 0)
        self.short_coins = [col for col in self.pos_df.columns
                            if self.pos_df[col].sum() < 0]
        self.coins4ma = coins.split()

    def check_cache(self):

        dir_name = "__" + ".".join(__file__.split(".")[:-1]) + "__"
        self.cache_filepath = os.path.join(dir_name, "prev")

        check1 = check2 = False

        if not os.path.isfile(self.cache_filepath):
            if not os.path.isdir(dir_name):
                os.mkdir(dir_name)
        else:
            self.prev = pd.read_pickle(self.cache_filepath)
            prev_df, curr_df = self.prev.pos_df, self.pos_df
            check1 = True

        if check1:
            cond1 = set(prev_df.index) >= set(curr_df.index)
            cond2 = set(prev_df.columns) >= set(curr_df.columns)
            if cond1 and cond2:
                self.whitelist += ["global_price_df", "price_df"]
                check2 = True

        if check2:
            cond11 = list(prev_df.index) == list(curr_df.index)
            cond12 = list(prev_df.columns) == list(curr_df.columns)
            cond1 = cond11 and cond12 and np.all(prev_df == curr_df)
            cond2 = self.prev.tcost == self.tcost
            if cond1 and cond2:
                self.whitelist += ["cumpnl_se", "pnl_dict", "coin_df"]
        if check2:
            cond1 = self.prev.coins4ma == self.coins4ma
            cond2 = self.ma
            if cond1 and cond2:
                self.whitelist += ["ma_df", "coins4ma"]

    def recall(self):
        for key in self.whitelist:
            print "[Visual.recall] recalling "+key
            if key == "price_df":
                price_df = self.global_price_df[self.pos_df.columns]
                price_df = self.filter_df(
                    price_df, 1, self.idx_first-self.min_time_delta*2, self.idx_last
                )
                setattr(self, key, price_df)
            else:
                prev_val = getattr(self.prev, key)
                setattr(self, key, prev_val)
    
    def memorize(self):
        keys = [key for key in self.checklist if key not in self.whitelist]
        for key in keys:
            if key:
                print "[Visual.memorize] Memorizing "+key
            if self.meta[key]:
                func = getattr(self, self.meta[key]["func"])
                args = self.meta[key]["args"]
                func(*args)
        pd.to_pickle(self, self.cache_filepath)

    def set_price_df(self):

        def get_name(ohlcv_path):
            file_name = os.path.split(ohlcv_path)[1]
            s = file_name
            s = s.replace("-OHLCV_SPOT", "").replace(".csv", "")
            l = s.split("_")
            return "-".join([l[1], l[2], l[0]])

        def get_price_se(name):
            df = pd.read_csv(path_se[name])
            if len(df) != 0:
                df.index = df["time"]
                se = df["close"]
                se.name = name
                return se

        ohlcv_dir = os.path.abspath("./data/ohlcv/")
        paths = [os.path.join(ohlcv_dir, exchange)
                 for exchange in self.exchanges]
        ohlcv_paths = [os.path.join(path, ohlcv)
                       for path in paths
                       for ohlcv in os.listdir(path)]

        path_dic = {get_name(path_): path_ for path_ in sorted(ohlcv_paths)}
        path_se = pd.Series(path_dic)[self.pos_df.columns]
        price_ses = [get_price_se(name) for name in path_se.index]
        price_df = pd.concat(price_ses, axis=1)

        p_df = pd.DataFrame({col: [0] for col in price_df.columns})
        p_price_df = pd.concat([p_df, price_df])
        price_df = p_price_df.fillna(method="ffill")[1:]
        
        price_df = self.filter_df(
            price_df, 1, self.idx_first-self.min_time_delta*2, self.idx_last
        )
        self.global_price_df = price_df
        self.price_df = price_df

    def set_cumpnl_se(self):
        
        def get_pnl_se(i):
            prevRow = p_pos_df.iloc[i]
            currRow = p_pos_df.iloc[i+1]
            prevPrice = price_df.iloc[i]
            currPrice = price_df.iloc[i+1]
            valueChange = currRow-prevRow
            raw_pnl = prevRow*((currPrice/prevPrice)-1.0)
            pnl_se = raw_pnl-sum(valueChange.abs())*tcost
            return pnl_se

        pos_df, price_df, tcost = self.pos_df, self.price_df, self.tcost
        p_pos_df = pd.concat([pos_df[:1], pos_df])
        ses = [get_pnl_se(i) for i in range(len(pos_df))]
        cumpnl_se = reduce(lambda x, y: x+y, ses)
        self.cumpnl_se = cumpnl_se

    def set_pnl_dict(self, *func_names):

        def sort_largest():
            print "[Visual.get_pnl_df] Sorting from largest coin ..."
            ranked_coin = sorted(self.pos_df.columns,
                                 reverse=True,
                                 key=lambda col: self.pos_df[col].sum())
            return ranked_coin

        def sort_most():
            print "[Visual.get_pnl_df] Sorting by most profitable coin ..."
            ranked_coin = sorted(self.cumpnl_se.index,
                                 reverse=True,
                                 key=lambda index: self.cumpnl_se[index])
            return ranked_coin

        def sort_least():
            print "[Visual.get_pnl_df] Sorting by least profitable coin ..."
            ranked_coin = sorted(self.cumpnl_se.index,
                                 key=lambda index: self.cumpnl_se[index])
            return ranked_coin

        def get_coin_se(ranked_coins, name):
            for short_coin in self.short_coins:
                ranked_coins.remove(short_coin)
            return pd.Series(ranked_coins, name=name)[:5]

        def get_pnl_df(cols):
            pos_df = self.pos_df.loc[:, cols]
            price_df = self.price_df.loc[:, cols]
            pnl_df = get_stats(pos_df,
                               price_df,
                               self.booksize,
                               tcost=self.tcost)
            tvr_se = GET_INTTVR_SE(pos_df, price_df, cols)
            more_df = pd.concat([tvr_se], axis=1)
            pnl_df = pd.concat([pnl_df, more_df], axis=1)
            return pnl_df

        coin_ses = [get_coin_se(eval(name+"()"), name) for name in func_names]
        self.coin_df = pd.concat(coin_ses, axis=1) if len(coin_ses) else None
        self.pnl_dict = {name: get_pnl_df(self.coin_df[name])
                         for name in func_names}
        self.pnl_dict["sort_default"] = get_pnl_df(self.pos_df.columns)

    def set_ma_df(self):

        cols = self.coins4ma
        pos_df = self.pos_df.loc[:, cols]
        price_df = self.price_df.loc[:, cols]
        pnl_df = get_stats(pos_df,
                           price_df,
                           self.booksize,
                           tcost=self.tcost)

        inttvr_se = GET_INTTVR_SE(pos_df, price_df, cols)
        more_df = pd.concat([inttvr_se], axis=1)
        pnl_df = pd.concat([pnl_df, more_df], axis=1)
        self.ma_df = pnl_df
    
    def filter_df(self, df, interval=None, start_time=None, end_time=None):

        interval = interval if interval else self.interval
        start_time = start_time if start_time else self.start_time
        end_time = end_time if end_time else self.end_time
        
        interval = self.min_time_delta*interval
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)

        time_indices, time_index = [], start_time
        n_step = (end_time-start_time)//interval
        for _ in range(n_step):
            time_index += interval
            time_indices.append(time_index)

        df.index = pd.to_datetime(df.index)
        return df.loc[time_indices]

    def draw_plot(self, title, option=""):

        def draw_sub_plot():
            df = self.filter_df(self.pnl_dict["sort_default"])
            plt.figure(figsize=(12, 5))
            ncol, nrow, margin = 1, 2, 0.05
            w = (1-margin*2)/ncol
            h = (1-margin)/nrow
            l = b = margin
            p0 = plt.axes([l, b+h*1, w, h])
            p0.plot(df["WinRatio"])
            p0.legend(["WinRatio"])
            p1 = plt.axes([l, b+h*2, w, h])
            p1.plot(df["DD"])
            p1.legend(["DD"])
            p1.get_xaxis().set_visible(False)
            p2 = plt.axes([l, b+h*3, w, h])
            p2.plot(df[["CumPnL", "maxi", "mini"]])
            p2.legend(["CumPnL", "maxi", "mini"])
            p2.get_xaxis().set_visible(False)
            plt.title("Maximum Drawdown and WinRtio", fontsize=20)
            plt.show()

        if not option:
            draw_sub_plot()
        default_df = self.pnl_dict["sort_default"]
        if option:
            coin_names = ", ".join(self.coin_df[option])
            description = "The first is left."
            df = self.pnl_dict[option].copy()
            df["Booksize"] = [df["Booksize"][i]-default_df["Short"][i]
                              for i in df.index]
        else:
            coin_names = ""
            description = ""
            df = self.pnl_dict["sort_default"].copy()

        df["Short"] = [-i for i in default_df["Short"]]
        df = self.filter_df(df)

        plt.figure(figsize=(12, 9))
        ncol, nrow, margin = 2, 3, 0.05
        w = (1 - margin * 2) / ncol
        h = (1 - margin) / nrow
        l = b = margin
        start = self.idx_first-self.min_time_delta*10
        end = self.idx_last+self.min_time_delta*10
        axis = [start, end, None, None]
        
        p1 = plt.axes([l, b, w, h])
        p1.plot(df["Std"])
        p1.legend(["Std"])
        plt.axis(axis)

        p2 = plt.axes([l*2+w, b, w, h])
        p2.plot(df[["Booksize", "Long", "Short"]])
        p2.legend(["Booksize", "Long", "Short"])
        plt.axis(axis)

        p3 = plt.axes([l, b+h, w, h])
        p3.plot(df["intTvr"])
        p3.legend(["intTvr"])
        plt.axis(axis)
        p3.get_xaxis().set_visible(False)

        p4 = plt.axes([l*2+w, b + h, w, h])
        p4.plot(df["Sharpe"])
        p4.legend(["Sharpe"])
        plt.axis(axis)
        p4.get_xaxis().set_visible(False)

        p5 = plt.axes([l, b+h*2, w, h])
        p5.plot(df["CumPnL"])
        p5.legend(["CumPnL"])
        plt.axis(axis)
        p5.get_xaxis().set_visible(False)

        p6 = plt.axes([l*2+w, b+h*2, w, h])
        p6.set_axis_off()
        p6.text(0, 0.6, title, fontsize=20)
        p6.text(0, 0.3, coin_names, fontsize=10)
        p6.text(0, 0.2, description, fontsize=10)

        plt.axis()

        plt.show()

    def draw_ma_plot(self, n):
        
        def apply_ma(se, n=n):
            index, name = se.index.copy(), se.name
            se = se[::-1]
            ma_list = [se[i:i+n].mean() for i in range(len(se))][::-1]
            ma_se = pd.Series(ma_list, index=index, name=name)
            return ma_se

        def get_stdshp_ses(pnl_se, ret_se, n=n, rfr=0):
            index, l = pnl_se.index.copy(), len(pnl_se)
            pnl_se = pnl_se[::-1]
            std_list = [np.std(pnl_se[i:i+n]) for i in range(l)][::-1]
            std_se = pd.Series(std_list, index=index, name="intStd")
            shp_list = [(ret_se[i]-rfr)/std_se[i] if std_se[i] else
                        float("Inf")
                        for i in range(l)]
            shp_se = pd.Series(shp_list, index=index, name="intShp")
            return std_se, shp_se

        df = self.filter_df(self.ma_df)
        std_se, shp_se = get_stdshp_ses(df["PnL"], apply_ma(df["IntRet"]))

        plt.figure(figsize=(12, 8))
        ncol, nrow, margin = 1, 4, 0.05
        w = (1 - margin * 2) / ncol
        h = (1 - margin) / nrow
        l = b = margin

        p1 = plt.axes([l, b+h, w, h])
        p1.plot(apply_ma(df["intTvr"]))
        p1.legend(["intTvr"])

        p2 = plt.axes([l, b+h*2, w, h])
        p2.plot(apply_ma(df["PnL"]))
        p2.legend(["intPnL"])
        p2.get_xaxis().set_visible(False)
        
        p3 = plt.axes([l, b+h*3, w, h])
        p3.plot(std_se)
        p3.legend(["intStd"])
        p3.get_xaxis().set_visible(False)

        p4 = plt.axes([l, b+h*4, w, h])
        p4.plot(shp_se)
        p4.legend(["intShp"])
        p4.get_xaxis().set_visible(False)

        title = ", ".join(self.coins4ma)
        plt.title(title, fontsize=20)

        plt.show()


def GET_INTTVR_SE(pos_df, price_df, coin_names):

    def calc_tvr(pair):
        prev_index, curr_index = pair
        prevRow = padded_df.loc[prev_index]
        currRow = padded_df.loc[curr_index]
        prev_booksize = sum(prevRow.abs())
        change = sum((currRow-prevRow).abs())
        tvr = (0 if not change else
                1 if not prev_booksize else
                change/prev_booksize)
        return tvr

    pos_df = pos_df[coin_names]
    padded_df = pd.concat([pos_df[:1], pos_df])
    padded_df.index = price_df.index

    index_pairs = zip(padded_df.index[:-1], padded_df.index[1:])
    tvrs = [calc_tvr(pair) for pair in index_pairs]
    tvr_se = pd.Series(tvrs, index=pos_df.index, name="intTvr")

    return tvr_se


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Visualize the results of [run.py].'
    )
    parser.add_argument(
        '-i',
        '--interval',
        help='The observation interval(minute). (ex. 5)',
        type=int
    )
    parser.add_argument(
        '-s',
        '--start',
        help='Start-time of visualization. (ex. 2017-01-01)',
        type=str
    )
    parser.add_argument(
        '-e',
        '--end',
        help='End-time of visualization. (ex. 2017-01-01)',
        type=str
    )
    parser.add_argument(
        '-a',
        '--alpha',
        help='CSV file of pos data (alpha). (ex. **/filepath.csv)',
        type=str
    )
    parser.add_argument('--tcost', type=float, default=0.0)
    parser.add_argument('--ma', type=bool, default=False)
    parser.add_argument('--range', type=int, default=None)
    parser.add_argument('--coins', type=str, default=None)
    args = parser.parse_args()

    INTERVAL = args.interval
    START_TIME = args.start
    END_TIME = args.end
    POS_FILEPATH = args.alpha
    TCOST = args.tcost
    MA = args.ma
    RANGE = args.range
    COINS = args.coins

    vis = Visual(INTERVAL, START_TIME, END_TIME, POS_FILEPATH, TCOST,
                 MA, COINS)

    vis.draw_plot("Output")
    vis.draw_plot("For Top 5 Largest Long Coins", "sort_largest")
    vis.draw_plot("For Top 5 Most Profitable Long Coins", "sort_most")
    vis.draw_plot("For Top 5 Least Profitable Long Coins", "sort_least")
