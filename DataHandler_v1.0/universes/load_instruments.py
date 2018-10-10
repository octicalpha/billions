import os,sys
import pandas as pd
from bittrex_util import BittrexWrapper as _BittrexWrapper
from krakenex import API
import requests
import ccxt

"""
Need to get startdate for other instruments
"""

class LoadInstruments:
    def __init__(self,inst,top_uni,bot_uni):
        self.inst = inst
        self.top_uni = top_uni
        self.bot_uni = bot_uni
        
    def get_binance(self):
        b = ccxt.binance()
        data = b.load_markets()
        data = pd.DataFrame(data)
        for col in data:
            startdate = 'NaN'
            ticker = col
            ticker_name = ticker.split("/")
            if ticker_name[0] == 'BCC':
                ticker_name[0] = 'BCH'
            ticker_name = ticker_name[0] + '-' + ticker_name[1] + '-BINA'
            ticker_temp = ticker_name.split('-')
            if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                self.inst.loc[len(self.inst)] = [startdate,ticker_name,'BINA']
        return self.inst

    def get_bittrex(self):
        b = _BittrexWrapper('','')
        data = b.get_market_summaries_dict()
        data = pd.DataFrame(data)
        for col in data:
            startdate = data[col]['Created'][:10]
            ticker = data[col]['MarketName']
            ticker_name = ticker.split("-")
            if ticker_name[1] == 'BCC':
                ticker_name[1] = 'BCH'
            ticker_name = ticker_name[1] + '-' + ticker_name[0] + '-BTRX'
            ticker_temp = ticker_name.split('-')
            if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                self.inst.loc[len(self.inst)] = [startdate,ticker_name,'BTRX']
        return self.inst

    def get_kraken(self):
        api = API()
        k = api.query_public(method='AssetPairs')['result']
        data = pd.DataFrame(k)
        for col in data:
            startdate = 'NaN'
            ticker = data[col]['altname'].split(".")
            if ticker[0][:4] in ['DASH','USDT']: # for pairs with 4 char
                pricing_curr = data[col]['altname'][-3:]
                if pricing_curr == 'XBT':
                    pricing_curr = 'BTC'
                ticker_name = ticker[0][:4] + '-' + pricing_curr + '-KRKN'
            else:
                pricing_curr = ticker[0][-3:]
                if pricing_curr == 'XBT':
                    pricing_curr = 'BTC'
                base_curr = ticker[0][:3]
                if base_curr == 'XBT':
                    base_curr = 'BTC'
                ticker_name = base_curr + '-' + pricing_curr + '-KRKN'
            if self.inst.loc[len(self.inst)-1]['ticker'] !=  ticker_name:
                ticker_temp = ticker_name.split('-')
                if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                    self.inst.loc[len(self.inst)] = [startdate,ticker_name,'KRKN']
        return self.inst

    def get_gemini(self):
        g = requests.get('https://api.sandbox.gemini.com/v1/symbols').json()
        data = pd.DataFrame(g)
        for index, row in data.iterrows():
            startdate = 'NaN'
            ticker_name = data[0][index][:3].upper() + '-' + data[0][index][-3:].upper() + '-GMNI'
            ticker_temp = ticker_name.split('-')
            if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                self.inst.loc[len(self.inst)] = [startdate,ticker_name,'GMNI']
        return self.inst

    def get_gdax(self):
        k = requests.get('https://api.gdax.com//products').json()
        data = pd.DataFrame(k)
        for index, row in data.iterrows():
            startdate = 'NaN'
            ticker_name = data['id'][index] + '-GDAX'
            ticker_temp = ticker_name.split('-')
            if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                self.inst.loc[len(self.inst)] = [startdate,ticker_name,'GDAX']
        return self.inst

    # Korean Exchanges

    def get_bithumb(self):
        k = requests.get('https://api.bithumb.com/public/ticker/all').json()
        data = pd.DataFrame(k)
        for row in data[:-1].index:
            startdate = 'NaN'
            ticker_name = row + '-KRW' + '-BTHM'
            ticker_temp = ticker_name.split('-')
            if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                self.inst.loc[len(self.inst)] = [startdate,ticker_name,'BTHM']
        return self.inst

    def get_coinone(self):
        k = requests.get('https://api.coinone.co.kr/ticker?currency=all').json()
        data = pd.DataFrame(k)
        for col in data:
            if col not in ['errorCode','result','timestamp']:
                startdate = 'NaN'
                ticker_name = col.upper() + '-KRW' + '-CONE'
                ticker_temp = ticker_name.split('-')
                if ticker_temp[0] not in self.top_uni and ticker_temp[1] not in self.bot_uni:
                    self.inst.loc[len(self.inst)] = [startdate,ticker_name,'CONE']
        return self.inst

    def get_korbit(self):
        """
        API update to come with full list of tickers
        """
        data = ['btc','eth','etc','xrp','bch']
        for col in data:
            startdate = 'NaN'
            ticker_name = col.upper() + '-KRW' + '-KBIT'
            ticker_temp = ticker_name.split('-')
            if ticker_temp[0] in self.top_uni and ticker_temp[1] in self.bot_uni:
                self.inst.loc[len(self.inst)] = [startdate,ticker_name,'KBIT']
        return self.inst

    def run(self,remove_file,output):
        """
        run() - runs ftn to create all instruments we will be including in total universe
        """
        filename = output + 'inst.csv'
        if remove_file and os.path.isfile(filename):
                os.remove(filename)
        if os.path.isfile(filename):
            inst = pd.read_csv(filename,index_col=0)
        else:
            self.inst = self.get_binance()
            self.inst = self.get_bittrex()
            self.inst = self.get_kraken()
            self.inst = self.get_gemini()
            self.inst = self.get_gdax()
            self.inst = self.get_bithumb()
            self.inst = self.get_coinone()
            self.inst = self.get_korbit()
            self.inst.to_csv(filename)
        return self.inst

