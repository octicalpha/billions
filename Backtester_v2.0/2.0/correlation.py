import pandas as pd
import numpy as np
import os
import re
from matplotlib import pyplot as plt
import argparse


class Corr(object):

    def __init__(
        self, pnl_dir_path, pnl_filepath, pos_dir_path, pos_filepath, range_
    ):

        self.meta_info = {
            "pnl": {
                "dir_path": pnl_dir_path,
                "filepath": pnl_filepath,
                "range": 0
            },
            "pos": {
                "dir_path": pos_dir_path,
                "filepath": pos_filepath,
                "range": range_
            }
        }

        self.pnl_df, self.pnl_se = self.get_sequences("pnl")
        self.pos_df, self.pos_se = self.get_sequences("pos")

        self.result_dict = {
            "PNL": self.get_corr_tuples(self.pnl_df, self.pnl_se),
            "POS": self.get_corr_tuples(self.pos_df, self.pos_se)
        }

    def get_sequences(self, scope):
        
        def get_filepaths():
            p = re.compile(".*_"+scope+".csv")
            all_names = os.listdir(dir_path)
            filenames = sum([p.findall(s) for s in all_names], [])
            return [os.path.join(dir_path, s) for s in filenames]
        
        def get_se(filepath):
            p = re.compile(".*/(.*)_"+scope+".csv")
            df = pd.read_csv(filepath, index_col=0)[-range_:]
            se = df["CumPnL"] if scope == "pnl" else df.mean()
            se.name = p.findall(filepath)[0]
            return se
        
        meta = self.meta_info
        dir_path = meta[scope]["dir_path"]
        pivot_filepath = meta[scope]["filepath"]
        range_ = meta[scope]["range"]
        
        filepaths = get_filepaths()
        pivot_se = get_se(pivot_filepath)
        
        ses = [get_se(filepath) for filepath in filepaths]
        df = pd.concat(ses, axis=1).drop([pivot_se.name], axis=1)
        
        return df, pivot_se

    def get_corr_tuples(self, sequence_df, pivot_se):

        def calc_corr(name):
            ses = [pivot_se, sequence_df[name]]
            df = pd.concat(ses, axis=1).dropna()
            corr_mat = np.corrcoef(df[pivot_se.name], df[name])
            return corr_mat[1, 0]
        
        corr_dict = {name: calc_corr(name) for name in sequence_df.columns}
        names = sorted(sequence_df.columns, key=lambda name: corr_dict[name])
        corr_tuples = [(name, corr_dict[name]) for name in names]

        return corr_tuples

    def display(self, scope):
        
        def draw_hist():
            plt.hist(map(lambda x: x[1], corr_tuples))
            plt.show()
        
        def _make_it_readable(t):
            name = t[0]+" "*(max_name_length-len(t[0]))
            corr =str(t[1])
            return name + " | " + corr

        def print_report():
            print(scope+" Max 5")
            for t in corr_tuples[:-6:-1]:
                print(_make_it_readable(t))
            print(scope+" Min 5")
            for t in corr_tuples[:5]:
                print(_make_it_readable(t))
            
        corr_tuples = self.result_dict[scope]
        max_name_length = max(map(lambda x: len(x[0]), corr_tuples))

        draw_hist()
        print_report()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--pnl_dir', type=str)
    parser.add_argument('--pnlfile', type=str)
    parser.add_argument('--pos_dir', type=str)
    parser.add_argument('--posfile', type=str)
    parser.add_argument('--range', type=int)
    args = parser.parse_args()

    INTERVAL = args.interval

    PNL_DIR_PATH = args.pnl_dir
    PNL_FILEPATH = args.pnlfile
    POS_DIR_PATH = args.pos_dir
    POS_FILEPATH = args.posfile
    RANGE = args.range

    corr = Corr(PNL_DIR_PATH, PNL_FILEPATH, POS_DIR_PATH, POS_FILEPATH, RANGE)

    corr.display("PNL")
    corr.display("POS")