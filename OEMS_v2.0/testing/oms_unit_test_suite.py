import unittest

import os.path, sys
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
from oms_client import OMSClient

import ccxt

startBalances = []

class OMSTests(unittest.TestCase):
    def test_exchange(self):
        #test safe buy
        #self.assertEqual(1,2)    
        #test safe sell
        self.assertEqual(1,1)
        #self.assertEqual('foo'.upper(), 'FOO')

def get_balances(omsClient, exchange, marketName):
    baseSym,quoteSym = marketName.split('/')
    balances = omsClient.get_balances(exchange)
    return balances[baseSym]['free'], balances[quoteSym]['free']

def test_market_orders(exchange, keypath, name, url, testMarketName, quoteTestQuantity):
    pass

def test_rebalancing(exchange, keypath, name, url):
    pass

def test_limit_orders(exchange, keypath, name, url, testMarketName, quoteTestQuantity):
    print('Testing safe limit orders on {}'.format(exchange))

    omsClient = OMSClient(keypath, name, url)

    baseSym,quoteSym = testMarketName.split('/')
    _,quoteQuantity0 = get_balances(omsClient, exchange, testMarketName)

    if(quoteQuantity0 < quoteTestQuantity):
        raise NameError('Need {} more {} to test.'.format(quoteTestQuantity-quoteQuantity0,quoteSym))

    publicAPI = getattr(ccxt,exchange)()

    #test limit buy
    print('Testing limit buy.')
    highestBid = publicAPI.fetch_ticker(testMarketName)['bid']
    buyPrice = highestBid*.95
    baseQuantityToTrade = quoteTestQuantity/highestBid
    orderResult = omsClient.order(exchange,'limit','buy', testMarketName, baseQuantityToTrade, buyPrice)
    assert(orderResult['status']=='open')

    #cancel order
    cancelResult = omsClient.cancel_order(exchange, orderResult['id'])
    assert(cancelResult['success']==True)

    #market buy to test limit sell
    dummyPrice = 0 
    orderResult = omsClient.order(exchange,'market','buy', testMarketName, baseQuantityToTrade, dummyPrice, False)

    #test limit sell
    print('Testing limit sell.')
    lowestAsk = publicAPI.fetch_ticker(testMarketName)['ask']
    sellPrice = lowestAsk*1.05
    orderResult = omsClient.order(exchange,'limit','sell', testMarketName, baseQuantityToTrade, sellPrice)
    assert(orderResult['status']=='open')

    #cancel order
    cancelResult = omsClient.cancel_order(exchange, orderResult['id'])
    assert(cancelResult['success']==True)

    #market sell to reset
    dummyPrice = 0
    orderResult = omsClient.order(exchange,'market','sell', testMarketName, baseQuantityToTrade, dummyPrice, False)


def test_safe_market_orders(exchange, keypath, name, url, testMarketName, quoteTestQuantity):
    print('Testing safe market orders on {}'.format(exchange))

    omsClient = OMSClient(keypath, name, url)

    baseSym,quoteSym = testMarketName.split('/')
    baseQuantity0,quoteQuantity0 = get_balances(omsClient, exchange, testMarketName)

    if(quoteQuantity0 < quoteTestQuantity):
        raise NameError('Need {} more {} to test.'.format(quoteTestQuantity-quoteQuantity0,quoteSym))

    publicAPI = getattr(ccxt,exchange)()

    #test market buy safe
    print('Testing safe market buy.')
    dummyPrice = 0
    baseQuantityToTrade = quoteTestQuantity/(publicAPI.fetch_ticker(testMarketName)['ask']*1.05)
    orderResult = omsClient.order(exchange,'market','buy', testMarketName, baseQuantityToTrade, dummyPrice, False)

    baseQuantity1,quoteQuantity1 = get_balances(omsClient, exchange, testMarketName)

    #check base/quote quantities
    assert(round(orderResult['amount'],5)==round(baseQuantity1-baseQuantity0,5))
    assert(round(baseQuantity1-baseQuantity0,5)==round(baseQuantityToTrade,5))
    assert(round(orderResult['quote_spent_or_gained'],5)==round(quoteQuantity0-quoteQuantity1,5))

    #check fee calculations
    print('Testing fee calculations.')
    taker_fee=omsClient.get_fees(exchange,testMarketName)['taker']
    assert(round(orderResult['cost']*taker_fee,6)==round(orderResult['fee']['cost'],6))

    #test market sell safe
    print('Testing safe market sell.')
    orderResult = omsClient.order(exchange,'market','sell', testMarketName, baseQuantityToTrade, dummyPrice, False)

    baseQuantity2,quoteQuantity2 = get_balances(omsClient, exchange, testMarketName)

    #check base/quote quantities
    assert(round(orderResult['amount'],5)==round(baseQuantity1-baseQuantity2,5))
    assert(round(baseQuantity1-baseQuantity2,5)==round(baseQuantityToTrade,5))
    assert(round(orderResult['quote_spent_or_gained'],5)==round(quoteQuantity2-quoteQuantity1,5))

    #check fee calculations
    print('Testing fee calculations.')
    assert(round(orderResult['amount']*orderResult['average']*taker_fee,6)==round(orderResult['fee']['cost'],6))

    print('Done')

if __name__ == '__main__':
    keypath = 'kenns_private_key.txt'
    name = 'kenneth'
    url = 'http://127.0.0.1:8888'
    #test_safe_market_orders('bittrex',keypath,name,url,'BTC/USDT', 20)
    test_limit_orders('bittrex', keypath, name, url, 'BTC/USDT', 20)
    print('Done')
    input()


