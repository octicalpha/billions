"""
Run the backtester_v2.0 ie. AltSim
"""

import pandas as pd
import argparse, importlib, os
from datetime import  *
from util.apply import *
from stats_new_float import *

def run():
    """
    --start : startdate of the simulation
    --end : enddate of the simulation
    --uni : universe name to be simulated
    --book : booksize to be tested, stats will calculate based on the starting book size
    --bookfloat : let booksize float, stats calculate differently if booksize floats, useful for event driven type models
    --local : we may consider loading data locally to avoid overload on db or reduce time consumption
    """
    
    parser = argparse.ArgumentParser(description='Run backtest on generate.py')
    parser.add_argument('-s','--start', help='start date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-e','--end', help='end date of universe creation ex. 2017-01-01.', type=str)
    parser.add_argument('-f','--file', help='file directory of generate.py', type=str, default='generate.py')
    parser.add_argument('-b','--book', help='booksize to be tested', type=int, default='1000000')
    parser.add_argument('--tcost', help='assign tcost', type=float, default='0.0')
    parser.add_argument('-o','--output_dir', help='dir where files will be saved', type=str, default='pnl/')
    parser.add_argument('--dload', help='True: download data, False: no download', default=False, type=str2bool,nargs='?',const=True)
    parser.add_argument('--app_file', help='True: append to existing pnl/pos', default=False, type=str2bool,nargs='?',const=True)
    args = parser.parse_args()

    startdate = args.start
    enddate = args.end
    file_name = args.file
    book = args.book
    dload = args.dload
    app_file = args.app_file
    tcost = args.tcost
    output_dir = args.output_dir
    altsim_dir = os.path.dirname(os.path.realpath(__file__))

    position_file = os.path.join(altsim_dir, output_dir, file_name[:-3] + '_pos.csv')
    pnl_file = os.path.join(altsim_dir, output_dir, file_name[:-3] + '_pnl.csv')
    px_file = os.path.join(altsim_dir, output_dir, file_name[:-3] + '_px.csv')
    file_dir = os.path.join(altsim_dir, file_name)

    #init
    spec = importlib.util.spec_from_file_location("", file_dir)
    alphaModule = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(alphaModule)
    AlphaClass = alphaModule.AlphaMain(startdate,enddate,book)

    #load data (more reasonable to do it this way for gridsearching.. not so much for a single run)
    dataHandlerDict = AlphaClass.load_data(altsim_dir, dload)
    params = AlphaClass.get_params_for_single_run()
    alpha = AlphaClass.generate(dataHandlerDict, params)

    if app_file:
        pos_old = pd.read_csv(position_file, index_col=0)
        pnl_old = pd.read_csv(pnl_file, index_col=0)
        px_old = pd.read_csv(px_file, index_col=0)
        last_old = pnl_old.index[-1]
        cumpnl_last = pnl_old.loc[last_old]['CumPnL']
        stats, px_exe = get_stats(alpha,AlphaClass.ohlcv_close,book,0.0,tcost=tcost)
        mask = (alpha.index > last_old)
        if sum(mask) > 0 :
            pos_new = alpha.loc[mask]
            pnl_new = stats.loc[mask]
            px_new = px_exe.loc[mask]
            pos_old = pos_old.append(pos_new)
            pnl_old = pnl_old.append(pnl_new)
            px_old = px_old.append(px_new)
            #import pdb; pdb.set_trace()
            #pnl_old['CumPnL'].iloc[-1] = cumpnl_last + pnl_old['PnL'].iloc[-1]
        if os.path.isfile(position_file):
            os.remove(position_file)
        if os.path.isfile(pnl_file):
            os.remove(pnl_file)
        if os.path.isfile(px_file):
            os.remove(px_file)
        save_file(position_file,pos_old,'Position')
        save_file(pnl_file,pnl_old,'PnL')
        save_file(px_file,px_old,'Prices')
    else:
        cumpnl_last = 0.0
        stats, px_exe = get_stats(alpha,AlphaClass.ohlcv_close,book,cumpnl_last,tcost=tcost)
        if os.path.isfile(position_file):
            os.remove(position_file)
        if os.path.isfile(pnl_file):
            os.remove(pnl_file)
        if os.path.isfile(px_file):
            os.remove(px_file)
        save_file(position_file,alpha,'Position')
        save_file(pnl_file,stats,'PnL')
        save_file(px_file,px_exe,'Prices')


def save_file(output_dir,file,name):
    file.to_csv(output_dir)
    print('[SAVE] {} file saved in {}'.format(name,output_dir))

def init_run():
    print('[ALTSIM] Initializing AltSim v2.0')

def str2bool(v):
    if v.lower() in ('yes', 'true', 'True', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'False','f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

if __name__ == '__main__':
    run()
