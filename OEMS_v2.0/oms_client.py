import requests
from fastecdsa import ecdsa, keys
from hashlib import sha384 as sha
import time
import json
import pandas as pd


class OMSClient:
    def __init__(self, keypath, name, url, botName, recordInDb, sendFailEmail):
        with open(keypath, "r") as f:
            self.privateKey = long(f.readline().strip())

        self.name = name
        self.verifySkeleton = "I {} am verified to trade at {}"
        self.url = url
        self.botName = botName
        self.recordInDb = recordInDb
        self.sendFailEmail = sendFailEmail

    def _api_query(self, resource, params):
        # convert non json serializable objects to strings
        for i, param in enumerate(params['args']):
            if (type(param) is pd.Timestamp):
                params['args'][i] = str(param)
        timestamp = time.time()
        message = sha(self.verifySkeleton.format(self.name, timestamp)).hexdigest()
        r, s = ecdsa.sign(message, self.privateKey, hashfunc=sha)
        post = {
            'name': self.name,
            'botName': self.botName,
            'recordInDb': self.recordInDb,
            'timestamp': timestamp,
            'sendFailEmail': self.sendFailEmail,
            'params': json.dumps(params),
            'r': r,
            's': s
        }
        url = '{}/{}'.format(self.url, resource)
        return requests.post(url, data=post).json()

    def verify(self):
        params = {'args': []}
        return self._api_query('verify', params)

    def market_order(self, exchange, side, marketSym, amount):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'market_order',
                                         'args': [side,
                                                  marketSym,
                                                  amount]})

    def limit_order(self, exchange, side, marketSym, amount, rate):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'limit_order',
                                         'args': [side,
                                                  marketSym,
                                                  amount,
                                                  rate]})

    def smart_order(self,
                    exchange,
                    startCurrency,
                    endCurrency,
                    startCurrencyAmount):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'smart_order',
                                         'args': [startCurrency,
                                                  endCurrency,
                                                  startCurrencyAmount]})

    def liquidate(self,
                  exchange,
                  coinToLiquidateTo,
                  coinsToLiquidate,
                  minBalanceUnit,
                  minBalance,
                  raiseMarketOfflineError):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'liquidate',
                                         'args': [coinToLiquidateTo,
                                                  coinsToLiquidate,
                                                  minBalanceUnit,
                                                  minBalance,
                                                  raiseMarketOfflineError]})

    def rebalance(self,
                  exchange,
                  idealSingleUnitLSBalance,
                  commonCurrency,  # TODO add checks on this... if commonCurrency can = reserveCurrency it should
                  reserveCurrency,
                  cushionBounds,
                  usingBNBForFees,
                  BNBBounds,
                  altSimPriceData,
                  mode,
                  sanityCheck,
                  plannedFail=False):

        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'rebalance',
                                         'args': [self.botName,
                                                  exchange,
                                                  idealSingleUnitLSBalance,
                                                  commonCurrency,
                                                  reserveCurrency,
                                                  cushionBounds,
                                                  usingBNBForFees,
                                                  BNBBounds,
                                                  altSimPriceData,
                                                  mode,
                                                  sanityCheck,
                                                  plannedFail]})

    def large_smart_order(self, exchangesToStartCurrencies, exchangesToTradingFees, endCurrency,
                          worstPrice, startAmount, mode, debugArgs):
        return self._api_query('multiexchangetrade', {'exchanges': exchangesToStartCurrencies.keys(),
                                                      'apiFunc': 'large_smart_order',
                                                      'args': [self.botName, exchangesToStartCurrencies,
                                                               exchangesToTradingFees, endCurrency,
                                                               worstPrice, startAmount, mode, debugArgs]})

    def update_ls_balance_via_trades(self, exchange, orders, commonCurrency, reserveCurrency, usingBNBForFees,
                                     storeInDb=True):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'update_ls_balance_via_trades',
                                         'args': [self.botName,
                                                  exchange, orders, commonCurrency, reserveCurrency, usingBNBForFees,
                                                  storeInDb]})

    def update_cushion_balance_via_trades(self, exchange, orders, commonCurrency, reserveCurrency,
                                          storeInDb=True):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'update_cushion_balance_via_trades',
                                         'args': [self.botName,
                                                  exchange, orders, commonCurrency, reserveCurrency,
                                                  storeInDb]})

    def store_ls_balance(self, exchange, lsBalance, lastTradeTime):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'store_ls_balance',
                                         'args': [self.botName,
                                                  exchange,
                                                  lsBalance,
                                                  lastTradeTime]})

    def store_borrow_balance(self, exchange, borrowBalance, lastWithdrawalDepositTime):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'store_borrow_balance',
                                         'args': [self.botName,
                                                  exchange,
                                                  borrowBalance,
                                                  lastWithdrawalDepositTime]})

    def store_cushion_balance(self, exchange, cushionBalance, lastTradeTime):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'store_cushion_balance',
                                         'args': [self.botName,
                                                  exchange,
                                                  cushionBalance,
                                                  lastTradeTime]})

    def store_reserve_currency_added(self, exchange, reserveCurrencyAdded, lastTradeTime):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'store_reserve_currency_added',
                                         'args': [self.botName,
                                                  exchange,
                                                  reserveCurrencyAdded,
                                                  lastTradeTime]})

    def get_last_ls_balance(self, exchange):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_last_ls_balance',
                                               'args': [self.botName,
                                                        exchange]})

    def get_last_borrow_balance(self, exchange):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_last_borrow_balance',
                                               'args': [self.botName,
                                                        exchange]})

    def get_last_cushion_balance(self, exchange):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_last_cushion_balance',
                                               'args': [self.botName,
                                                        exchange]})

    def get_last_reserve_currency_added(self, exchange):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_last_reserve_currency_added',
                                               'args': [self.botName,
                                                        exchange]})

    def update_balances(self, exchange, how, reserveCurrency,
                        lsBalances=None,
                        initialBorrowBalances=None,
                        cushionBalances=None,
                        reserveCurrencyAdded=None):

        balanceDicts = [lsBalances, initialBorrowBalances, cushionBalances, reserveCurrencyAdded]
        balanceDicts = [bd for bd in balanceDicts if bd is not None]
        if (how == 'replace' and len(balanceDicts) != 4):
            raise NameError('All balances must supplied on initialization.')
        elif (how == 'append' and len(balanceDicts) == 0):
            raise NameError('At least one balance type must be updated.')
        elif(how not in ['append', 'replace']):
            raise NameError('"how" can be either "append" or "replace". Current value: {}'.format(how))

        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'update_balances',
                                         'args': [self.botName, exchange, how, reserveCurrency,
                                                  lsBalances, initialBorrowBalances, cushionBalances, reserveCurrencyAdded
                                                  ]})

    #TODO shuold update borrow and reserve currency...
    def update_borrow_balance_via_wds(self, exchange):
        wdsResults = self._api_query('accountInfo', {'exchange': exchange,
                                                     'apiFunc': 'get_wds_since_last_borrow_balance_store',
                                                     'args': [self.botName, exchange]})
        chosenWds = []
        for wd in wdsResults['result']:
            yn = raw_input('Use this transfer to update borrow balance? {} (y/n): '.format(wd))
            if (yn == 'y'):
                chosenWds.append(wd)
            elif (yn != 'n'):
                raise NameError('Invalid input: {}'.format(yn))
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'update_borrow_balance_via_wds',
                                         'args': [self.botName, exchange, chosenWds]})

    def get_ls_positions_via_balances(self,
                                      exchange,
                                      currencyUnit,
                                      idealSingleUnitLSBalance):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_ls_positions_via_balances',
                                               'args': [self.botName,
                                                        exchange,
                                                        currencyUnit,
                                                        idealSingleUnitLSBalance]})

    def get_ls_positions_via_db(self,
                                exchange,
                                botName,
                                currencyUnit,
                                idealSingleUnitLSBalance):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_ls_positions_via_db',
                                               'args': [botName,
                                                        exchange,
                                                        currencyUnit,
                                                        idealSingleUnitLSBalance]})

    def get_balances(self, exchange):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_balances',
                                               'args': []})

    # todo test with bittrex.... do round of rebalancing... and see if getting orders gets the same thing
    def get_past_orders(self,
                        exchange,
                        startTime,
                        endTime,
                        marketSymbols,
                        inclusive=False):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_past_orders',
                                               'args': [startTime,
                                                        endTime,
                                                        marketSymbols,
                                                        inclusive]})

    # todo test with all exchanges
    def get_withdrawal_deposit_history(self, exchange, startTime, endTime, coinSymbols, inclusive=False):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_withdrawal_deposit_history',
                                               'args': [startTime,
                                                        endTime,
                                                        coinSymbols,
                                                        inclusive]})

    # todo take timestamp out
    def get_order_stats(self, exchange, orderID, timestamp, marketSym):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_order_stats',
                                               'args': [orderID, timestamp, marketSym]})

    # todo only add for gdax and binance
    def get_real_order_info(self, exchange, order):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_real_order_info',
                                               'args': [order]})

    # todo add to binance
    def get_fees(self, exchange, marketSym):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'get_fees',
                                               'args': [marketSym]})

    # todo test with to binance
    def check_health(self, exchange):
        return self._api_query('accountInfo', {'exchange': exchange,
                                               'apiFunc': 'check_health',
                                               'args': []})

    def cancel_order(self, exchange, orderID, marketSym=None):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'cancel_order',
                                         'args': [orderID, marketSym]})

    def cancel_open_orders(self, exchange, marketSyms):
        return self._api_query('trade', {'exchange': exchange,
                                         'apiFunc': 'cancel_open_orders',
                                         'args': [marketSyms]})

    # todo add if we have time
    def withdrawal(self, startExchange, endExchange, currency, amount):
        pass