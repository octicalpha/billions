import sys
sys.path.insert(0,'/home/ubuntu/dev/crypto_oms')
from analytics.oms_data_analytics import OMSDataAnalytics
import argparse
import pandas as pd


def init_borrow_balances(OMSDataAnalyzer): #TODO and start time
    # {u'ETC': 12626.99407494, u'ETH': 359.98990574000004, u'BCH': 295.0059, u'XRP': 245489.0, u'BTC': 31.6402}

    startTimes = {'kraken':pd.Timestamp('2018-05-08 07:27:00.190469'),'binance':pd.Timestamp('2018-05-08 07:23:09.427506')} #todo get start times for each exchange... ensure its in_z
    OMSDataAnalyzer.init_bot_start_times(startTimes)

    OMSDataAnalyzer.init_borrow_balances('kraken',
                                         {"ETC": 12668.99407494, "ETH": 359.98990574000004, "BCH": 295.0059,
                                          "XRP": 169000.0, "BTC": 32.4402, 'USD': 0},
                                         startTimes['kraken'])
    OMSDataAnalyzer.init_borrow_balances('binance',
                                         {"NEO": 0, "XLM": 0, "KMD": 0, "BCH": 0, "STEEM": 0, "BCD": 0, "EOS": 0,
                                          "ETC": 0, "ETH": 0, "IOST": 0, "DGD": 0, "QTUM": 0, "STRAT": 0, "LSK": 0,
                                          "HSR": 0, "WTC": 0, "XMR": 0, "PPT": 0, "AION": 0, "ONT": 0, "WAN": 0,
                                          "AE": 0, "ZRX": 0, "DASH": 0, "TRX": 0, "ICX": 0, "ADA": 0, "VEN": 0,
                                          "BNB": 0, "ZIL": 0, "XRP": 0, "XVG": 0, "ZEC": 0, "LRC": 0, "OMG": 0,
                                          "SNT": 0, "BTS": 0, "XEM": 0, "LTC": 0, "BTG": 0, "WAVES": 0, "ARK": 0,
                                          "BTC": 5, "USDT": 0},
                                         startTimes['binance'])

def run():
    parser = argparse.ArgumentParser(description='pnl tracker')
    parser.add_argument('-m', '--mode',choices=['current','backfill'] , default='current', type=str)
    args = parser.parse_args()
    mode=args.mode

    OMSDataAnalyzer = OMSDataAnalytics('jaekimopt003')

    if(mode=='current'):
        OMSDataAnalyzer.get_current_pnl()
    elif(mode=="backfill"):
        pass

if __name__ == '__main__':
    run()