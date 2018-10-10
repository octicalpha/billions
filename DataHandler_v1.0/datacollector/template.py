import utils.postgresConnection as pgConn
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os
import requests
import datetime
from datetime import datetime, timedelta
import time
import argparse

'''
Set constants:

'''
constants = 0

class DataModule(object):
    def __init__(self,parse_imp,refresh,path,save):
        self.parse_imp = parse_imp
        self.refresh = refresh
        self.path = path
        self.save = save
        '''
        Set class init variables to define specific universe to use
        '''
        self.include_bool = False
        self.include_base = []
        self.include_quote = []
        self.exclude_bool = False
        self.exclude_base = []
        self.exclude_quote = []
        self.set_exch = []

    def get_data(uni):
        """
        def get_data()
            Input: Uni list
            Usage: Call data within this function.
        """
        raise NotImplementedError

    def update_save_data(self,old_data,data,exsiting):
        """
        def update_save_data()
            Input:
                old_data = preexisting data
                data = new data to append / save
                existing = bool defining if there is preexisting data
            Usage: Save / update data
        """
        data = self.reformat_data(data)
        data['in_z'] = pd.to_datetime(time.strftime("%Y%m%dT%H%M%S"))
        if exsiting:
            old_data = old_data.append(data, ignore_index=True)
            old_data.to_csv(filename)
        else:
            data.to_csv(filename)

    def reformat_data(data):
        """
        def reformat_data()
            Input:
                data = new data to append / save
            Usage: Return formated data for save / storage
        """
        raise NotImplementedError
        return data

    def run(uni):
        """
        def run()
            Usage: Class DataCollector will run this function to retrieve data.
            If any parameters required, use parse_imp to import from DataCollector, else ignore.
        """
        raise NotImplementedError
        return data