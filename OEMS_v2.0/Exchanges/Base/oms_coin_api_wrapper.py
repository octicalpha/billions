import pandas as pd
from .coin_api_client import CoinAPIv1
import time
from urllib2 import HTTPError
import datetime

class OMSCoinAPIWrapper:
    def __init__(self, apiKey, exchangeName, productionUrls, log=None):
        self.productionUrls = self.format_productionUrls(productionUrls)
        self.apiKey = apiKey
        self.productionUrlIdx = 0
        self.coinAPIClient=CoinAPIv1(apiKey, self.productionUrls[0])
        self.exchangeName=exchangeName.upper()
        self.log = log

    def log_it(self, level, msg, printMsg=True, *args, **kwargs):
        if (printMsg):
            print(datetime.datetime.now().strftime('%Y/%m/%d %I:%M:%S %p'), msg)
        if self.log is None:
            print(level, msg, args, kwargs)
        if level.lower() == 'info':
            self.log.info(msg, *args, **kwargs)
        elif level.lower() == 'error':
            self.log.error(msg, *args, **kwargs)
        elif level.lower() == "debug":
            self.log.debug(msg, *args, **kwargs)
        else:
            self.log.log(level, msg, *args, **kwargs)

    def format_productionUrls(self, productionUrls):
        productionUrls = productionUrls.split(',')
        productionUrlsNew = []
        for productionUrl in productionUrls:
            productionUrlsNew.append(str(productionUrl)+'/v1%s')
        return productionUrlsNew

    def coinapi_failover(self, f, *args, **kwargs):
        wait_time = kwargs.get('waitTime', 2)
        result = None
        for idxInc in range(len(self.productionUrls)):
            try:
                result = f(*args, **kwargs)
                break
            except HTTPError as e:
                if(e.code/100 == 5 or e.code == 404):
                    self.log_it('info', '==========New Production URL==========')
                    self.log_it('info', str(e))
                    self.log_it('info', 'Old Production URL: {}'.format(self.productionUrls[self.productionUrlIdx]))
                    self.productionUrlIdx = (self.productionUrlIdx+idxInc+1) % len(self.productionUrls)
                    self.log_it('info', 'New Production URL: {}'.format(self.productionUrls[self.productionUrlIdx]))
                    self.coinAPIClient = CoinAPIv1(self.apiKey, self.productionUrls[self.productionUrlIdx])
                    if(idxInc == len(self.productionUrls)-1):
                        raise e
                else:
                    raise e

            time.sleep(wait_time)

        return result

    def ccxt_market_sym_to_coinapi(self, ccxtSym):
        b,q=ccxtSym.split('/')
        return "{}_SPOT_{}_{}".format(self.exchangeName,b,q)

    def coinapi_market_sym_to_ccxt(self, coinapiSym):
        _,_,b,q=coinapiSym.split('_')
        #todo figure out way to convert coinapi symbols to ccxt symbols without calling ccxt.load_markets
        if(b=='BCC'):
            b='BCH'
        return b+'/'+q

    def coinapi_ticker_to_ccxt(self, coinapiTicker):
        ccxtTicker={}

        if('last_trade' in coinapiTicker):
            ccxtTicker['last'] = coinapiTicker['last_trade']['price']
            ccxtTicker['timestamp']=pd.Timestamp(coinapiTicker['last_trade']['time_exchange']).timestamp()*1000
            ccxtTicker['datetime']=pd.Timestamp(coinapiTicker['last_trade']['time_exchange']).tz_convert(None)
        else:
            ccxtTicker['last'] = (coinapiTicker['ask_price']+coinapiTicker['bid_price'])/2.0
            ccxtTicker['timestamp'] = pd.Timestamp(coinapiTicker['time_exchange']).timestamp() * 1000
            ccxtTicker['datetime'] = pd.Timestamp(coinapiTicker['time_exchange']).tz_convert(None)

        ccxtTicker['askVolume']=coinapiTicker['ask_size']
        ccxtTicker['symbol']=self.coinapi_market_sym_to_ccxt(coinapiTicker['symbol_id'])
        ccxtTicker['bidVolume']=coinapiTicker['bid_size']
        ccxtTicker['ask']=coinapiTicker['ask_price']
        ccxtTicker['bid']=coinapiTicker['bid_price']
        return ccxtTicker

    def fetch_tickers(self, ccxtSymbols=None):
        if(ccxtSymbols is not None):
            coinapiSymbols=[]
            for ccxtSymbol in ccxtSymbols:
                coinapiSymbols.append(self.ccxt_market_sym_to_coinapi(ccxtSymbol))
            filterVal=','.join(coinapiSymbols)
        else:
            filterVal=self.exchangeName

        coinapiTickers=self.coinapi_failover(self.coinAPIClient.quotes_current_data_all,{'filter_symbol_id': filterVal})

        ccxtTickers = {}
        for coinapiTicker in coinapiTickers:
            ccxtTicker = self.coinapi_ticker_to_ccxt(coinapiTicker)
            ccxtTickers[ccxtTicker['symbol']] = ccxtTicker

        return ccxtTickers