import pandas as pd
import datetime
import numpy as np
import random
from util.postgresConnection import query


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
    return pnl - (tvr * booksize * tcost)


def turnover(currRow, prevRow):
    """
    % [-2,2] of how much of the booksize we are trading
    """
    prev_booksize = sum(prevRow.abs())
    change = sum((currRow-prevRow).abs())
    tvr = (0 if not change else
           1 if not prev_booksize else
           change/prev_booksize)
    return tvr


def infoRatio(portRet, benchReturn, diffs, stdtype):
    diff = portRet - benchReturn
    diffs.append(diff)
    return diff / np.std(diffs, ddof=1 if stdtype == "sample" else 0)


def PnL(currRow, currPrice, prevRow, prevPrice):
    output = prevRow * ((currPrice / prevPrice) - 1.0)
    output = output.fillna(0)
    output.replace(np.inf, 0, inplace=True)
    return output

def get_stats(
    alphas, prices, booksize_full, rfr=0.0, stdtype="sample", tcost=0.0
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

    prevRow, prevPrice = alphas.iloc[0], prices.iloc[0]
    tvr = []
    tvr_daily = 0
    fullpnl = []
    cumPnL = 0.0
    days = 0
    startday = alphas.index[0]
    index_prev = 0
    for index, row in alphas.iterrows():
        if startday.date() < row.name.date():
            startday = row.name
            days += 1
            tvr.append(tvr_daily)
            tvr_daily = 0
        
        #import pdb; pdb.set_trace()
        try:
            currPrice = prices.loc[index]
        except:
            currPrice = prices.loc[index_prev]

        currRow = row
        booksize = sum(currRow.abs())
        tvr_now = turnover(currRow, prevRow)
        intPnL = sum(PnL(currRow, currPrice, prevRow, prevPrice))
        intPnL = get_pnl_tcost(tvr_now, intPnL, booksize, tcost)
        cumPnL += intPnL
        cumRet = (booksize_full+cumPnL)/booksize_full-1.0
        fullpnl.append(intPnL)
        tvr_daily += tvr_now
        try:
            res["IntRet"][index] = (booksize_full+intPnL)/booksize_full-1.0
        except:
            res["IntRet"][index] = np.nan
        try:
            res["Ret"][index] = cumRet*(365/days)
        except:
            res["Ret"][index] = np.nan
        if len(res["IntRet"].values()) != 1:
            res["Std"][index] = np.std(fullpnl,
                                       ddof=1 if stdtype == "sample" else 0)
            res["Sharpe"][index] = (res["Ret"][index] - rfr)/res["Std"][index]
        else:
            res["Std"][index] = np.nan
            res["Sharpe"][index] = np.nan
        try:
            tvr_temp = tvr
            tvr_temp.append(tvr_daily)
            res["Tvr"][index] = sum(tvr_temp)/len(tvr_temp)
        except:
            res["Tvr"][index] = np.nan
        res["Booksize"][index] = sum(currRow.abs())
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