import pandas as pd
import datetime
import numpy as np
import argparse
import os
from util.postgresConnection import query
from util.apply import *
from build_core import UniHandler, AlphaHandler
from data.modules.ohlcv_interval_DH import DataHandler

def get_slippage(amount_invested, market):
    """
    if we are including slippage, get bps and calculate

    Maybe something that is variable?:

    Idea (Ken):
        market_sizes = {
            (BTC,USD,GDAX) = 100,000
            (BTC,ETH,GDAX) = 50,000
            (BTC,USD,Kraken) = 15,000
            ...
        }
        market_size = market_sizes[market]
        random_slippage = Normal( 5bp, amount_invested/market_size )
        tcost = tcosts[market] # 25bp for gemini, 0/25 for gdax, 2/12 for kraken
        slippage = tcosts + Max( random_slippage, -5BP ) # automatically lose 5bp at best, lets be conservative
    """


def get_pnl_tcost(tvr, pnl, booksize, tcost):
    """
    get pnl after tcost
    """
    return pnl - (tvr * tcost)


def turnover(currRow, prevRow, currPrice, prevPrice):
    """
    Total volume per interval of how much we are trading
    """
    prev_booksize = sum(prevRow.abs())
    tvr = sum(((currRow/currPrice)-(prevRow/prevPrice)).abs() * currPrice)
    #tvr = (if).replace(np.inf, prev_booksize, inplace=True)
    #tvr = (0 if not change_usd else 1 if not prev_booksize else change_usd)
    return tvr

def infoRatio(portRet, benchReturn, diffs, stdtype):
    diff = portRet - benchReturn
    diffs.append(diff)
    return diff / np.std(diffs, ddof=1 if stdtype == "sample" else 0)


def PnL(currRow, currPrice, prevRow, prevPrice):
    ret = (currPrice / prevPrice) - 1.0
    ret = ret.fillna(0)
    ret.replace(np.inf, 0, inplace=True)
    output = prevRow * ret
    output = output.fillna(0)
    output.replace(np.inf, 0, inplace=True)
    
    return output

def get_stats(
    alphas, prices, booksize_full, cumpnl_last, interval, rfr=0.0, stdtype="sample", tcost=0.0
):
    """
    :param alphas:
    :param prices: pd.df of coin prices over time
    :param rfr: risk free rate
    :param stdtype: sample/population standard dev
    :return: pd.df of complete portfolio stats
    """

    print "[STATS] Initializing ..."
    res = {
        "Booksize": dict(),
        "Long": dict(),
        "Short": dict(),
        "CumPnL": dict(),
        "PnL": dict(),
        "IntRet": dict(),
        "Ret": dict(),
        "Sharpe": dict(),
        # "DDD":dict(),
        "Std": dict(),
        "Tvr": dict(),
    }
    previousVal = 0
    prevRow, prevPrice = alphas.iloc[0], prices.iloc[0]
    tvr = []
    tvr_daily = 0
    fullpnl = []
    cumPnL = cumpnl_last
    days = 0
    startday = alphas.index[0]
    output_prices = pd.DataFrame(columns=alphas.columns,index=alphas.index)
    interval = alphas.index[1] - alphas.index[0]
    interval = interval.total_seconds() / 60.0
    for index, row in alphas.iterrows():
        if startday.date() < row.name.date():
            startday = row.name
            days += 1
            tvr.append(tvr_daily)
            tvr_daily = 0
        currPrice = prices.loc[index]
        output_prices.loc[index] = currPrice
        currRow = row
        currRow = currRow.fillna(0)
        prevRow = prevRow.fillna(0)
        booksize = sum(currRow.abs())
        tvr_now = turnover(currRow, prevRow, currPrice, prevPrice)
        intPnL = sum(PnL(currRow, currPrice, prevRow, prevPrice))
        intPnL = get_pnl_tcost(tvr_now, intPnL, booksize, tcost)
        cumPnL += intPnL
        cumRet = (booksize_full+cumPnL)/booksize_full-1.0
        tvr_daily += tvr_now
        #import pdb; pdb.set_trace()
        try:
            res["IntRet"][index] = (booksize_full+intPnL)/booksize_full-1.0
        except:
            res["IntRet"][index] = np.nan
        try:
            res["Ret"][index] = cumRet*(365/days)
        except:
            res["Ret"][index] = np.nan
        fullpnl.append(res["IntRet"][index])
        if len(res["IntRet"].values()) != 1:
            std_temp = np.std(fullpnl,ddof=0)
            res["Std"][index] = std_temp * np.sqrt(365*(1440/interval))
            res["Sharpe"][index] = (np.mean(fullpnl) - rfr)/std_temp * np.sqrt(365*(1440/interval))
        else:
            res["Std"][index] = np.nan
            res["Sharpe"][index] = np.nan
        try:
            tvr_temp = tvr
            tvr_temp.append(tvr_daily)
            res["Tvr"][index] = sum(tvr_temp)/len(tvr_temp)
        except:
            res["Tvr"][index] = np.nan
        res["Booksize"][index] = booksize
        res["Long"][index] = sum(currRow[currRow >= 0])
        res["Short"][index] = sum(currRow[currRow < 0])
        res["CumPnL"][index] = cumPnL
        res["PnL"][index] = intPnL
        prevRow = currRow
        prevPrice = currPrice

    df = pd.DataFrame(res)

    maxi_se = np.maximum.accumulate(df["CumPnL"])
    maxi_se.name = "maxi"

    p_maxi_list = [None] + maxi_se.tolist()
    pivots = [i for i in range(len(maxi_se))
              if p_maxi_list[i] != p_maxi_list[i+1]]
    pivots_p = pivots + [len(maxi_se)]
    mini_ses = [np.minimum.accumulate(df["CumPnL"][pivots_p[i]:pivots_p[i+1]])
                for i in range(len(pivots))]
    mini_se = pd.concat(mini_ses)
    mini_se.name = "mini"

    dd_se = (-(maxi_se-mini_se)/maxi_se).fillna(0)
    dd_se.name = "DD"

    wr_se = np.cumsum(df["PnL"] > 0)/range(1, len(df)+1)
    wr_se.name = "WinRatio"

    more_df = pd.concat([maxi_se, mini_se, dd_se, wr_se], axis=1)
    df = pd.concat([df, more_df], axis=1)

    return df

def save_file(output_dir,file,name):
    file.to_csv(output_dir)
    print '[SAVE] {} file saved in {}'.format(name,output_dir)

def apply_delay_keep(data,delay):
    """
    applies delay by variable delay to avoid forward-bias
    """
    output = data.shift(delay)
    return output

def run():
    parser = argparse.ArgumentParser(description='Run backtest on generate.py')
    parser.add_argument('-s','--start', help='start date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-e','--end', help='end date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-f','--file', help='file directory of generate.py', type=str, default='generate.py')
    parser.add_argument('-b','--book', help='booksize to be tested', type=int, default='1000000')
    parser.add_argument('-i','--interval', help='interval', type=int, default='5')
    parser.add_argument('--tcost', help='assign tcost', type=float, default='0.0')
    parser.add_argument('--altsim_dir', help='dir of altsim', type=str, default='')
    args = parser.parse_args()
    startdate = args.start
    enddate = args.end
    file_dir = args.file
    book = args.book
    interval = args.interval
    tcost = args.tcost
    altsim_dir = args.altsim_dir

    pnl_dir = file_dir[:-8] + '_pnlanalyze.csv'
    pos = pd.read_csv(file_dir, index_col=0)
    pos.index = pd.to_datetime(pos.index)
    #import pdb; pdb.set_trace()
    mask = (pos.index >= pd.to_datetime(startdate)) & (pos.index <= pd.to_datetime(enddate))
    pos = pos.loc[mask]
    
    # Cases where EXCH instruments are BTC/ETH etc underlying
    for i in pos:
        split_col = i.split('-')
        if split_col[2] == 'BTRX':
            pos['BTC-USDT-BTRX'] = np.nan
            break

    DH = DataHandler(startdate,enddate,1,5,False,altsim_dir)   
    ohlcv = DH.build_data(pos,'ohlcv','aws_exchanges')
    ohlcv_close = apply_to_matrix(ohlcv,'close','ticker',startdate,enddate,5)
    ohlcv_close = ohlcv_close.fillna(method='bfill')
    ohlcv_close = ohlcv_close.fillna(method='ffill')
    
    #ohlcv_close = apply_delay_keep(ohlcv_close,1)

    # Recalculate */BTC to */USDT for BTRX
    for col in ohlcv_close:
        if col.split('-')[2] == 'BTRX' and col != 'BTC-USDT-BTRX':
            x = ohlcv_close.loc[:,col]
            btc = ohlcv_close.loc[:,'BTC-USDT-BTRX']
            for i in x.index:
                #import pdb; pdb.set_trace()
                x.loc[i] = x.loc[i] * btc.loc[i]

    stats = get_stats(pos,ohlcv_close,book,0.0,interval,tcost=tcost)
    if os.path.isfile(pnl_dir):
        os.remove(pnl_dir)
    save_file(pnl_dir,stats,'PnL')

if __name__ == '__main__':
    run()
