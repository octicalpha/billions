from utils.postgresConnection import storeInDb, query
import time
import pandas as pd
import numpy as np
import os, sys
import argparse

class UploadUniverse:
    def __init__(self,dir,schema,table_name):
        self.dir = dir
        self.schema = schema
        self.table_name = table_name

    def last_db_date(self):
        q_data = """select * from {}.{} order by time desc limit 5""".format(self.schema,self.table_name)
        df = query(q_data, environment='aws_dev',dataframe=True)
        return df['time'][3]

    def store_daily_uni(self):
        df = pd.read_csv(self.dir, index_col=0)
        days_range = df.columns[3:]
        last_date = '2017-10-01'#self.last_db_date()
        mask = (days_range > last_date)
        if sum(mask) > 0 :
            days_range = days_range[mask]
            out_df = df[days_range]
            out_df = out_df.T
            temp = df.T.iloc[0]
            temp.name = None
            out_df.columns = temp
            out_df.insert(loc=0,column='time',value=out_df.index)
            storeInDb(out_df, tableName=self.table_name, environment='aws_dev', addMilestoneCol=False, schema=self.schema)
            
    def store_first_time(self):
        df = pd.read_csv(self.dir, index_col=0)
        out_df = df.T
        temp = out_df.iloc[0]
        temp.name = None
        out_df.columns = temp
        out_df.insert(loc=0,column='time',value=out_df.index)
        #import pdb; pdb.set_trace()
        storeInDb(out_df, tableName=self.table_name, environment='aws_dev', addMilestoneCol=False, schema=self.schema)

    def run_raw_data(self,option,path_dyn,init_bool):
        today = time.strftime("%Y-%m-%d")
        if init_bool:
            last_date = '2017-10-01'
        else:
            last_date = '2017-10-01'#self.last_db_date()
        #path_now = os.path.dirname(os.path.realpath(__file__))
        cmd = "/usr/bin/python " + path_dyn + "dynamic_uni.py -s " + last_date + " -e " + today + " -o " + option + " --path_dyn " + path_dyn
        os.system(cmd)

    def run_uni_data(self,option,unitype,days,num,path_dyn,init_bool):
        today = time.strftime("%Y-%m-%d")
        if init_bool:
            last_date = '2017-10-01'
        else:
            last_date = '2017-10-01'#self.last_db_date()
        #path_now = os.path.dirname(os.path.realpath(__file__))
        #last_date = self.last_db_date()
        cmd = "/usr/bin/python " + path_dyn + "dynamic_uni.py -s " + last_date + " -e " + today + " -o " + option + ' --' + unitype + ' True ' + '--' + unitype + 'n ' + num + ' -d ' + days + " --path_dyn " + path_dyn
        os.system(cmd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--dir',help='directory of uni file',type=str)
    parser.add_argument('-p','--path',help='directory of universes folder',type=str,default='universes/')
    parser.add_argument('-s','--schema',help='schemaname',type=str)
    parser.add_argument('-t','--table',help='table_name',type=str)
    parser.add_argument('-o','--option',help='volume,mktcap',type=str)
    parser.add_argument('--unitype',help='top,mincut',type=str)
    parser.add_argument('--days',help='days to smooth',type=str)
    parser.add_argument('--num',help='top=top N,mincut=min N in mn USD',type=str)
    parser.add_argument('-i','--init',help='initiation?',type=bool, default=False)
    args = parser.parse_args()

    uni_dir = args.dir
    up_uni = UploadUniverse(uni_dir,args.schema,args.table)
    up_uni.run_raw_data(args.option, args.path,args.init)
    up_uni.run_uni_data(args.option,args.unitype,args.days,args.num,args.path,args.init)
    if args.init:
        up_uni.store_first_time()
    else:
        up_uni.store_daily_uni()