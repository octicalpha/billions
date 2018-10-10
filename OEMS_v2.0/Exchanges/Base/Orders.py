import ccxt
import pandas as pd
import time

class OrderInfo:
    def __init__(self, side, marketSym, amount, initSizeSu, exchangeObj, worstPrice = None,
                 raiseMarketOfflineError=True):
        assert side in ['buy', 'sell']
        assert amount > 0
        assert initSizeSu is None or initSizeSu > 0

        self.side=side
        self.marketSym=marketSym
        self.remaining=amount
        self.initSizeSu=initSizeSu
        self.active=True
        self.exchangeObj=exchangeObj
        self.worstPrice = worstPrice
        self.results={'orderInfo':[],'errors':{'sellingTooMuch':[],
            'buyingTooMuch':[],
            'dustOrder':[],
            'marketOffline':[]}}
        self.placementInfo=None
        self.raiseMarketOfflineError=raiseMarketOfflineError

    def view_order(self):
        self.exchangeObj.log_it('info','Side: {}, Market: {}, Amount MU: {}, Amount: SU: {}'.format(self.side,
                                                                                                    self.marketSym,
                                                                                                    self.remaining,
                                                                                                    self.initSizeSu))

    def get_quote(self):
        return self.marketSym.split('/')[1]

    def get_base(self):
        return self.marketSym.split('/')[0]

    def get_rate(self, orderTry, tickers):
        if(self.side == 'buy'):
            worstPrice = tickers[self.marketSym]['ask']
            bestPrice = tickers[self.marketSym]['bid']
        else:
            worstPrice = tickers[self.marketSym]['bid']
            bestPrice = tickers[self.marketSym]['ask']

        price = None
        if(orderTry > 10):
            if(self.side == 'buy'):
                price = worstPrice*1.0005
            else:
                price = worstPrice*.9995
        elif(orderTry > 2):
            price = worstPrice
        else:
            bestPriceWeight = .5**(orderTry+1)
            worstPriceWeight = 1-bestPriceWeight
            price = worstPrice*worstPriceWeight+bestPrice*bestPriceWeight

        return price

    def place_order(self, orderTry, tickers):
        rate = self.get_rate(orderTry, tickers)

        if(self.worstPrice is not None):
            if ((self.side == 'buy' and rate > self.worstPrice) or
                    self.side == 'sell' and rate < self.worstPrice):
                self.exchangeObj.log_it('info', 'Reached specified worst price. Worst Price: {}, Cur Price: {}'.format(
                    self.worstPrice, rate
                ))
                self.active = False
                return

        self.exchangeObj.log_it('info', 'Trying... limit side={} sym={} amount={} rate={}'.format(self.side,
                                                                                                  self.marketSym,
                                                                                                  self.remaining,
                                                                                                  rate))

        try:
            orderInfo=self.exchangeObj.limit_order(self.side, self.marketSym, self.remaining, rate)

            self.placementInfo = {'orderInfo':orderInfo,
                'ticker':tickers[self.marketSym]}

        except ccxt.InvalidOrder as e:
            if(self.exchangeObj.is_dust_order_error(e)):
                self.results['errors']['dustOrder'].append({'side':self.side,
                    'marketSym':self.marketSym,
                    'amount':self.remaining})
            elif(self.exchangeObj.is_market_offline_error(e)):
                if(self.raiseMarketOfflineError):
                    raise e
                else:
                    curTime = pd.Timestamp.now()
                    self.results['marketOffline'].append({'marketSym':self.marketSym,
                        'time':str(curTime)})
            else:
                raise e
            self.active = False
        except ccxt.InsufficientFunds as e:
            remub = self.exchangeObj.get_balances()
            # check if selling "slightly" too much
            if (self.side == 'sell' and
                    self.remaining >= remub[self.get_base()]['free']*.99999 and
                    self.remaining < remub[self.get_base()]['free'] * 1.02):
                newAmount = remub['free'][self.get_base()] * .9999
                self.exchangeObj.log_it('info','Selling Too Much {}, Original Amt:{}, New Amt:{}, Rate:{}, Balance:{}'.format(
                    self.marketSym, self.remaining, newAmount, rate, remub[self.get_base()]
                ))
                self.results['errors']['sellingTooMuch'].append({'marketSym': self.marketSym,
                                                                 'orignalAmount': self.remaining,
                                                                 'newAmount': newAmount,
                                                                 'rate': rate,
                                                                 'reb': remub[self.get_base()]})
                self.remaining = newAmount
            #check if buying "slightly" too much
            elif(self.side == 'buy' and
                    self.remaining >= (remub[self.get_quote()]['free']/rate)*.98 and
                    self.remaining < (remub[self.get_quote()]['free']/rate)*1.02):
                newAmount =  self.remaining * .995
                self.exchangeObj.log_it('info',
                                        'Buying Too Much {}, Original Amt:{}, New Amt:{}, Rate:{}, Balance:{}'.format(
                                            self.marketSym, self.remaining, newAmount, rate, remub[self.get_quote()]
                                        ))
                self.results['errors']['buyingTooMuch'].append({'marketSym': self.marketSym,
                                                                 'orignalAmount': self.remaining,
                                                                 'newAmount': newAmount,
                                                                 'rate': rate,
                                                                 'reb': remub[self.get_quote()]})
                self.remaining = newAmount
            else:
                if(self.side == 'buy'):
                    self.exchangeObj.log_it('info',
                                            'Buying WAY Too Much {}, Original Amt:{}, Rate:{}, Balance:{}'.format(
                                                self.marketSym, self.remaining, rate, remub[self.get_quote()]
                                            ))
                else:
                    self.exchangeObj.log_it('info',
                                            'Selling WAY Too Much {}, Original Amt:{}, Rate:{}, Balance:{}'.format(
                                                self.marketSym, self.remaining, rate, remub[self.get_base()]
                                            ))
                raise e

    def check_if_order_went_through(self):
        if(self.placementInfo is not None):
            #get order results
            orderInfo = self.exchangeObj.check_if_order_went_through(self.placementInfo['orderInfo'],
                tries=1,
                sleepTime=.1,
                raisePartialFillError=False)
            orderInfo['ticker'] = self.placementInfo['ticker']
            self.results['orderInfo'].append(orderInfo)

            #update order state
            self.remaining = orderInfo['remaining']
            self.placementInfo = None
            if(orderInfo['remaining'] == 0):
                self.active=False

class OrderBatch:
    def __init__(self, orders, exchangeObj, tryFreq=5):
        self.orders = orders
        self.marketSyms = [order.marketSym for order in orders]
        self.exchangeObj = exchangeObj
        self.tryFreq=tryFreq
        self.active=True

    def view_batch(self):
        for orderInfo in self.orders:
            orderInfo.view_order()

    def done_executing(self):
        for order in self.orders:
            if(order.active):
                return False
        return True

    def combine_results(self):
        results={'orderInfo':[],'errors':{'sellingTooMuch':[],
            'buyingTooMuch':[],
            'dustOrder':[],
            'marketOffline':[]}}

        for orderInfo in self.orders:
            results['orderInfo']+=orderInfo.results['orderInfo']
            results['errors']['sellingTooMuch']+=orderInfo.results['errors']['sellingTooMuch']
            results['errors']['buyingTooMuch']+=orderInfo.results['errors']['buyingTooMuch']
            results['errors']['dustOrder']+=orderInfo.results['errors']['dustOrder']
            results['errors']['marketOffline']+=orderInfo.results['errors']['marketOffline']

        return results

    def execute_batch(self):
        orderTry = 0

        while not self.done_executing():
            self.exchangeObj.log_it('info','Batch Execution Order Try = {}'.format(orderTry))

            tickers = self.exchangeObj.get_tickers_safe(self.marketSyms)

            #place orders
            for orderInfo in self.orders:
                if(orderInfo.active):
                    time.sleep(self.exchangeObj.orderPlacementFreq)
                    orderInfo.place_order(orderTry, tickers)

            #wait a little while for orders to fill
            time.sleep(self.tryFreq)
            
            for orderInfo in self.orders:
                if(orderInfo.active):
                    orderInfo.check_if_order_went_through()

            #update order try 
            orderTry +=1

        self.active=False
        
        return self.combine_results()

    def get_net_volume(self, quote):
        netVol=0
        for order in self.orders:
            if(order.side=='buy' and order.get_quote()==quote):
                netVol+=order.initSizeSu
            elif(order.side=='sell' and order.get_quote()==quote):
                netVol-=order.initSizeSu
        return netVol