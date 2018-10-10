import sys, argparse
sys.path.insert(0,'/home/ubuntu/dev/crypto_oms')
from analytics.oms_data_analytics import OMSDataAnalytics
import pandas as pd

def run():
    parser = argparse.ArgumentParser(description='pnl tracker')
    parser.add_argument('-m', '--mode', choices=['currentPnL','getStatsJSON','getVisualizations'], default='currentPnL', type=str)
    args = parser.parse_args()
    mode=args.mode

    OMSDataAnalyzer = OMSDataAnalytics('jaekimopt005')

    if(mode=='currentPnL'):
        curPnl = OMSDataAnalyzer.get_current_pnl(pd.Timestamp('2018-08-16 01:13:00'))
        return curPnl
    elif(mode=="getStatsJSON"):
        OMSDataAnalyzer.stats_to_csv(pd.Timestamp('2018-08-16 01:13:00'), pd.Timestamp.now())
    elif(mode=='getVisualizations'):
        OMSDataAnalyzer.visualize_stats(pd.Timestamp('2018-08-16 01:13:00'), pd.Timestamp.now())

if __name__ == '__main__':
    run()