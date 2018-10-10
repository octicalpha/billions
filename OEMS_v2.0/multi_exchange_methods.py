from threading import Thread
import time
from utils.RouteFinder import MultiExchangeRouteFinder
from utils.print_util import get_info_logger
import datetime
import pandas as pd
import json
from sqlalchemy import create_engine
import os
from collections import defaultdict

class MultiExchangeMethods:
    def __init__(self, exchangesAPIs, db_environ_url="DATABASE_URL_AWS_DEV"):
        self.exchangeAPIs = exchangesAPIs
        self.log = get_info_logger('logs/multi_exchange' + str(time.time()) + ".log")
        self.DB_ENGINE = create_engine(os.environ.get(db_environ_url))
        
        self.balancesTableName = 'all_exchange_balances'
        
    def log_it(self, level, msg, printMsg=True, printOnNextLine=False, *args, **kwargs):
        if (printMsg):
            if(printOnNextLine):
                print(datetime.datetime.now().strftime('%Y/%m/%d %I:%M:%S %p'))
                print(msg)
            else:
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

    def get_ob_thread_function(self, exchange, market, obDict):
        b,q=market.split('/')
        obName = '{}_{}_{}'.format(exchange, b, q)
        obDict[obName] = self.exchangeAPIs[exchange].get_ob(market)

    def get_orderbooks(self, exchangeToMarket, obDict):
        threadList = []
        for exchange in exchangeToMarket:
            t = Thread(target=self.get_ob_thread_function, args=(exchange, exchangeToMarket[exchange], obDict))
            threadList.append(t)
            t.start()
        for t in threadList:
            t.join()

    def get_oderbooks_from_routes(self, routes, merf):
        maxRouteLen = max([len(route) for route in routes])
        exchangeToMarketForEachRouteStep = [None]*maxRouteLen
        for i in range(maxRouteLen):
            exchangeToMarketForEachRouteStep[i] = {}
        #query for 1 ob from an exchange at a time
        for route in routes:
            for i,routeAction in enumerate(route):
                e,b,q,_=merf.split_market_action(routeAction)
                exchangeToMarketForEachRouteStep[i][e] = '{}/{}'.format(b,q)
        obDict = {}
        for e2m in exchangeToMarketForEachRouteStep:
            self.get_orderbooks(e2m, obDict)
        #re-index obs by market action
        reIndexedObDict = {}
        for route in routes:
            for marketAction in route:
                e,b,q,_=merf.split_market_action(marketAction)
                curKey = '{}_{}_{}'.format(e,b,q)
                reIndexedObDict[marketAction]=obDict[curKey]

        return reIndexedObDict

    def check_currencies(self, exchangesToStartCurrencies, endCurrency):
        #check that markets exist
        for exchange,startCurrency in exchangesToStartCurrencies.items():
            if('{}/{}'.format(startCurrency, endCurrency) not in self.exchangeAPIs[exchange].allMarketSyms and
                    '{}/{}'.format(endCurrency, startCurrency) not in self.exchangeAPIs[exchange].allMarketSyms):
                raise NameError('A {}, {}, market does not exist on {}.'.format(startCurrency,endCurrency,exchange))

        #check start currencies
        startCurrency = None
        usdTypes = set(['USD', 'USDT', 'TUSD'])
        for exchange in exchangesToStartCurrencies:
            if (startCurrency is None):
                startCurrency = exchangesToStartCurrencies[exchange]
            else:
                if (startCurrency != exchangesToStartCurrencies[exchange]):
                    if (not (startCurrency in usdTypes and exchangesToStartCurrencies[exchange] in usdTypes)):
                        raise NameError('Invalid start currenceis {}.'.format(exchangesToStartCurrencies))

    def check_exchange_info(self, exchangesToStartCurrencies, exchangeToTradingFees):
        e2c = set(exchangesToStartCurrencies.keys())
        e2tf = set(exchangeToTradingFees.keys())
        if(len(e2c.difference(e2tf))>0 or len(e2tf.difference(e2c))>0):
            raise NameError("""Start currency dict keys and tradeing fees dict keys are different.\nStart
                            currency dict keys: {}, trade fees dict keys: {}'.format(e2c, e2tf))""")
            
    def store_balance(self, botName, balances):
        data = pd.DataFrame([{self.balancesTableName: json.dumps(balances), 'in_z': pd.Timestamp.now()}])
        data.to_sql(self.balancesTableName, self.DB_ENGINE, if_exists='replace', schema=botName, index=False)
        
    def get_last_balances(self, botName):
        sql = 'select * from "{}".{} order by in_z desc limit 1'.format(botName, self.balancesTableName)
        balanceInfo = pd.read_sql(sql, self.DB_ENGINE)
        balanceInfo = balanceInfo.loc[0].to_dict()
        balanceInfo[self.balancesTableName] = json.loads(balanceInfo[self.balancesTableName])
        return balanceInfo
    
    def get_cur_balances(self, exchanges):
        threads = []
        balanceDict = {}
        for exchange in exchanges:
            t = Thread(target=self.exchangeAPIs[exchange].get_balances, args=(balanceDict,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        return balanceDict

    def get_past_route_changes(self, merf, routes, originalBalances, curBalances):
        routeBalanceChanges = defaultdict(dict)
        for route in routes:
            exchange,_,_,_ = merf.split_market_action(route[0])
            routeChain = self.exchangeAPIs[exchange].routeFinder.get_route_chain(route)
            for coin in routeChain:
                routeBalanceChanges[exchange][coin] = curBalances[exchange][coin]['free']-originalBalances[exchange][coin]['free']
        return routeBalanceChanges

    def execute_routes_in_parallel(self, argsDict, resultsDict):
        threadList = []
        for exchange in resultsDict:
            t = Thread(target=self.exchangeAPIs[exchange].execute_routes, args=argsDict[exchange])
            threadList.append(t)
            t.start()
        for t in threadList:
            t.join()

    def log_balances(self, balances, exchangesToStartCurrencies, endCurrency):
        logStr = ''
        for exchange in balances:
            logStr+='Exchange: {}, Start Currency Balance: {}, End Currency Balance: {}\n'.format(exchange,
                                                                balances[exchange][exchangesToStartCurrencies[exchange]]['free'],
                                                                balances[exchange][endCurrency]['free'])
        self.log_it('info', logStr, printOnNextLine=True)

    def log_route_execution_results(self, resultsDict):
        logStr = ''
        for exchange in resultsDict:
            logStr+='Exchange: {}, Quantity History: {}\n'.format(exchange,
                                                                  resultsDict[exchange]['result']['routeQuantitiesHistory'])
        self.log_it('info', logStr, printOnNextLine=True)

    def large_smart_order(self, botName, exchangesToStartCurrencies, exchangeToTradingFees, endCurrency,
                                   worstPrice, startAmount, mode, debugArgs):

        self.log_it('info', '==========START LARGE SMART ORDER==========')

        allResults = {}

        exchanges = exchangesToStartCurrencies.keys()

        self.check_currencies(exchangesToStartCurrencies, endCurrency)
        self.check_exchange_info(exchangesToStartCurrencies, exchangeToTradingFees)

        if (mode == 'normal'):
            self.log_it('info', '==========Normal Mode==========')
            # store current exchange balances in DB
            originalBalances = self.get_cur_balances(exchanges)
            self.log_balances(originalBalances, exchangesToStartCurrencies, endCurrency)
            self.store_balance(botName, originalBalances)
        elif (mode == 'recover'):
            self.log_it('info', '==========Recover Mode==========')
            originalBalances = self.get_last_balances(botName)
            originalBalances = originalBalances[self.balancesTableName]
            self.log_it('info', 'Balances before failed attempt.')
            self.log_balances(originalBalances, exchangesToStartCurrencies, endCurrency)
            curBalances = self.get_cur_balances(exchanges)
            self.log_it('info', 'Balances after failed attempt.')
            self.log_balances(curBalances, exchangesToStartCurrencies, endCurrency)
        else:
            raise NameError('An invalid mode was given: {}, Valid modes are: {}.'.format(mode, ['normal', 'recover']))

        # find allocation across exchanges via route finder
        merf = MultiExchangeRouteFinder()

        # get routes
        exchangeRoutes = []
        for exchange in exchangesToStartCurrencies:
            routes = self.exchangeAPIs[exchange].routeFinder.find_routes(exchangesToStartCurrencies[exchange],
                                                                         endCurrency)
            exchangeRoutes.append(self.exchangeAPIs[exchange].choose_route(routes))

        recoveryResultsDict = None
        if (mode == 'recover'):
            routeBalanceChangesAllExchanges = self.get_past_route_changes(merf, exchangeRoutes, originalBalances,
                                                                          curBalances)
            self.log_it('info', 'Route Balance Changes: {}'.format(routeBalanceChangesAllExchanges))
            recoveryArgsDict = {}
            recoveryResultsDict = {}

            # get routes between 'stranded' coins and endCurrency for each exchange
            for exchange in routeBalanceChangesAllExchanges:
                routeBalanceChanges = routeBalanceChangesAllExchanges[exchange]
                recoveryStartQuantities = {}
                recoveryRoutes = []

                # get routes between 'stranded' coins and endCurrency for single exchange
                for coin in routeBalanceChanges:
                    if (coin != exchangesToStartCurrencies[exchange] and
                            coin != endCurrency and routeBalanceChanges[coin] > 0):
                        recoveryStartQuantities[coin] = routeBalanceChanges[coin]
                        routes = self.exchangeAPIs[exchange].routeFinder.find_routes(
                            coin, endCurrency)
                        recoveryRoutes.append(self.exchangeAPIs[exchange].choose_route(routes))

                # get args dict
                if (len(recoveryRoutes) > 0):
                    # make route batch structure
                    lenOfMaxRoute = max([len(route) for route in recoveryRoutes])
                    recoveryRouteBatches = []
                    for i in range(lenOfMaxRoute):
                        recoveryRouteBatches.append([])

                    # fill in route batch structure
                    for route in recoveryRoutes:
                        for i, marketAction in enumerate(route):
                            recoveryRouteBatches[i].append(marketAction)

                    # init args for parallel route execution
                    exchangeRecoverResultsDict = {}
                    recoveryResultsDict[exchange] = exchangeRecoverResultsDict
                    recoveryArgsDict[exchange] = (recoveryRouteBatches, recoveryStartQuantities, True, worstPrice, exchangeRecoverResultsDict)

            # execute recovery routes
            self.log_it('info', 'Recovery Route Execution Args: {}'.format(recoveryArgsDict))
            self.execute_routes_in_parallel(recoveryArgsDict, recoveryResultsDict)
            self.log_route_execution_results(recoveryResultsDict)

            # adjust start amount
            oldStartAmount = startAmount
            for exchange in exchanges:
                startAmount += routeBalanceChangesAllExchanges[exchange][exchangesToStartCurrencies[exchange]]
            self.log_it('info',
                        'Adjusting Start Amount. Old Start Amount: {}, New Start Amount: {}'.format(oldStartAmount,
                                                                                                    startAmount))
            self.log_it('info', '==========Recovery Finished==========')

        # start amount may be slightly less than 0
        if (startAmount < 0):
            startAmount = 0

        # get OB/trading fees
        obDict = self.get_oderbooks_from_routes(exchangeRoutes, merf)

        self.log_it('info', '==========Route Info==========')
        routeInfo, _ = merf.get_quantity_per_route(startAmount, obDict, exchangeRoutes, exchangeToTradingFees,
                                                   worstPrice)
        for i, route in enumerate(routeInfo['allRoutes']):
            self.log_it('info', 'Route: {}, Start Currency Amt To Allocate: {}, Route Is Open: {}'.format(
                route, routeInfo['amountPerRoute'][i], routeInfo['routeIsOpen'][i]
            ))
        mixedObStr = 'Multi Exchange OB\n'
        for level in routeInfo['combinedOrderBook']:
            mixedObStr += 'Market: {}, Price: {}, Amount: {}\n'.format(level[0], level[1], level[2])
        self.log_it('info', mixedObStr, printOnNextLine=True)

        if (debugArgs is not None):
            total = sum([amt for amt in routeInfo['amountPerRoute']])
            for i in range(len(routeInfo['amountPerRoute'])):
                routeInfo['amountPerRoute'][i] = (routeInfo['amountPerRoute'][i] / total) * debugArgs[
                    'actualStartAmount']

        # make smart orders w/ price + time limit (threads)
        self.log_it('info', '==========Start Execution==========')
        argsDict = {}
        resultsDict = {}
        for i, route in enumerate(routeInfo['allRoutes']):
            quantity = routeInfo['amountPerRoute'][i]
            if (quantity > 0):
                # reformat route (will eventually make this cleaner)
                routeBatches = []
                for marketAction in route:
                    routeBatches.append([marketAction])
                e, b, q, s = merf.split_market_action(route[0])
                startCurrency = exchangesToStartCurrencies[e]
                routeStartQuantities = {startCurrency: quantity}
                exchangeResultDict = {}
                resultsDict[e] = exchangeResultDict

                argsDict[e] = (routeBatches, routeStartQuantities, True, worstPrice, exchangeResultDict)

        # execute routes
        self.log_it('info', 'Route Execution Args: {}'.format(argsDict))
        self.execute_routes_in_parallel(argsDict, resultsDict)
        self.log_route_execution_results(resultsDict)
        resultsDict['routeInfo'] = routeInfo

        # store results
        allResults['normalResults'] = resultsDict
        allResults['recoveryResults'] = recoveryResultsDict

        # get final start/end amounts
        finalBalances = self.get_cur_balances(exchanges)
        startCurrencyChange = 0
        endCurrencyChange = 0
        for exchange in exchanges:
            startCurrencyChange += abs(originalBalances[exchange][exchangesToStartCurrencies[exchange]]['free'] -
                                       finalBalances[exchange][exchangesToStartCurrencies[exchange]]['free'])
            endCurrencyChange += abs(originalBalances[exchange][endCurrency]['free'] -
                                     finalBalances[exchange][endCurrency]['free'])
        allResults['startCurrencyAmt'] = startCurrencyChange
        allResults['endCurrencyAmt'] = endCurrencyChange
        self.log_it('info',
                    'Start Currency Amt: {}, End Currency Amt: {}'.format(startCurrencyChange, endCurrencyChange))

        self.log_it('info', '==========END LARGE SMART ORDER==========')
        return allResults

    def large_smart_order_original(self, botName, exchangesToStartCurrencies, exchangeToTradingFees, endCurrency,
                          worstPrice, maxTime, startAmount, mode, debugArgs):
        
        self.log_it('info', '==========START LARGE SMART ORDER==========')

        allResults = {}

        exchanges = exchangesToStartCurrencies.keys()

        self.check_currencies(exchangesToStartCurrencies, endCurrency)
        self.check_start_currencies(exchangesToStartCurrencies)
        self.check_exchange_info(exchangesToStartCurrencies, exchangeToTradingFees)
        
        if(mode=='normal'):
            self.log_it('info', '==========Normal Mode==========')
            #store current exchange balances in DB
            originalBalances = self.get_cur_balances(exchanges)
            self.log_balances(originalBalances, exchangesToStartCurrencies, endCurrency)
            self.store_balance(botName, originalBalances)
        elif(mode=='recover'):
            self.log_it('info', '==========Recover Mode==========')
            originalBalances = self.get_last_balances(botName)
            originalBalances = originalBalances[self.balancesTableName]
            self.log_it('info','Balances before failed attempt.')
            self.log_balances(originalBalances, exchangesToStartCurrencies, endCurrency)
            curBalances = self.get_cur_balances(exchanges)
            self.log_it('info','Balances after failed attempt.')
            self.log_balances(curBalances, exchangesToStartCurrencies, endCurrency)
        else:
            raise NameError('An invalid mode was given: {}, Valid modes are: {}.'.format(mode, ['normal', 'recover']))

        # find allocation across exchanges via route finder
        merf = MultiExchangeRouteFinder()
        
        #get routes
        exchangeRoutes = []
        for exchange in exchangesToStartCurrencies:
            routes = self.exchangeAPIs[exchange].routeFinder.find_routes(exchangesToStartCurrencies[exchange], endCurrency)
            exchangeRoutes.append(self.exchangeAPIs[exchange].choose_route(routes))

        recoveryResultsDict = None
        if(mode == 'recover'):
            routeBalanceChangesAllExchanges = self.get_past_route_changes(merf, exchangeRoutes, originalBalances, curBalances)
            self.log_it('info', 'Route Balance Changes: {}'.format(routeBalanceChangesAllExchanges))
            recoveryArgsDict = {}
            recoveryResultsDict = {}

            # get routes between 'stranded' coins and endCurrency for each exchange
            for exchange in routeBalanceChangesAllExchanges:
                routeBalanceChanges = routeBalanceChangesAllExchanges[exchange]
                recoveryStartQuantities = {}
                recoveryRoutes = []

                # get routes between 'stranded' coins and endCurrency for single exchange
                for coin in routeBalanceChanges:
                    if(coin != exchangesToStartCurrencies[exchange] and
                            coin != endCurrency and routeBalanceChanges[coin] > 0):
                        recoveryStartQuantities[coin] = routeBalanceChanges[coin]
                        routes = self.exchangeAPIs[exchange].routeFinder.find_routes(
                            coin, endCurrency)
                        recoveryRoutes.append(self.exchangeAPIs[exchange].choose_route(routes))

                #get args dict
                if(len(recoveryRoutes)>0):
                    #make route batch structure
                    lenOfMaxRoute = max([len(route) for route in recoveryRoutes])
                    recoveryRouteBatches = []
                    for i in range(lenOfMaxRoute):
                        recoveryRouteBatches.append([])

                    #fill in route batch structure
                    for route in recoveryRoutes:
                        for i, marketAction in enumerate(route):
                            recoveryRouteBatches[i].append(marketAction)

                    #init args for parallel route execution
                    exchangeRecoverResultsDict = {}
                    recoveryResultsDict[exchange] = exchangeRecoverResultsDict
                    recoveryArgsDict[exchange] = (recoveryRouteBatches, recoveryStartQuantities, True, None, exchangeRecoverResultsDict)

            #execute recovery routes
            self.log_it('info', 'Recovery Route Execution Args: {}'.format(recoveryArgsDict))
            self.execute_routes_in_parallel(recoveryArgsDict, recoveryResultsDict)
            self.log_route_execution_results(recoveryResultsDict)

            #adjust start amount
            oldStartAmount = startAmount
            for exchange in exchanges:
                startAmount += routeBalanceChangesAllExchanges[exchange][exchangesToStartCurrencies[exchange]]
            self.log_it('info', 'Adjusting Start Amount. Old Start Amount: {}, New Start Amount: {}'.format(oldStartAmount, startAmount))
            self.log_it('info', '==========Recovery Finished==========')

        #start amount may be slightly less than 0
        if(startAmount<0):
            startAmount=0

        #TODO add max time...
        startTime = time.time()

        # get OB/trading fees
        obDict = self.get_oderbooks_from_routes(exchangeRoutes, merf)
        
        self.log_it('info', '==========Route Info==========')
        routeInfo,_ = merf.get_quantity_per_route(startAmount, obDict, exchangeRoutes, exchangeToTradingFees, worstPrice)
        for i, route in enumerate(routeInfo['allRoutes']):
            self.log_it('info', 'Route: {}, Start Currency Amt To Allocate: {}, Route Is Open: {}'.format(
                route, routeInfo['amountPerRoute'][i], routeInfo['routeIsOpen'][i]
            ))
        mixedObStr = 'Multi Exchange OB\n'
        for level in routeInfo['combinedOrderBook']:
            mixedObStr+='Market: {}, Price: {}, Amount: {}\n'.format(level[0], level[1], level[2])
        self.log_it('info', mixedObStr, printOnNextLine=True)
        
        #TODO remove
        if(debugArgs is not None):
            total = sum([amt for amt in routeInfo['amountPerRoute']])
            for i in range(len(routeInfo['amountPerRoute'])):
                routeInfo['amountPerRoute'][i] = (routeInfo['amountPerRoute'][i]/total)*debugArgs['actualStartAmount']
        
        #make smart orders w/ price + time limit (threads)
        self.log_it('info', '==========Start Execution==========')
        argsDict = {}
        resultsDict = {}
        for i, route in enumerate(routeInfo['allRoutes']):
            quantity = routeInfo['amountPerRoute'][i]
            if (quantity > 0):
                #reformat route (will eventually make this cleaner)
                routeBatches = []
                for marketAction in route:
                    routeBatches.append([marketAction])
                e, b, q, s = merf.split_market_action(route[0])
                startCurrency = exchangesToStartCurrencies[e]
                routeStartQuantities = {startCurrency: quantity}
                exchangeResultDict = {}
                resultsDict[e] = exchangeResultDict
                
                argsDict[e] = (routeBatches, routeStartQuantities, True, None, exchangeResultDict)

        #execute routes
        self.log_it('info', 'Route Execution Args: {}'.format(argsDict))
        self.execute_routes_in_parallel(argsDict, resultsDict)
        self.log_route_execution_results(resultsDict)
        resultsDict['routeInfo'] = routeInfo

        #store results
        allResults['normalResults'] = resultsDict
        allResults['recoveryResults'] = recoveryResultsDict

        #get final start/end amounts
        finalBalances = self.get_cur_balances(exchanges)
        startCurrencyChange = 0
        endCurrencyChange = 0
        for exchange in exchanges:
            startCurrencyChange += abs(originalBalances[exchange][exchangesToStartCurrencies[exchange]]['free']-
                                       finalBalances[exchange][exchangesToStartCurrencies[exchange]]['free'])
            endCurrencyChange += abs(originalBalances[exchange][endCurrency]['free']-
                                       finalBalances[exchange][endCurrency]['free'])
        allResults['startCurrencyAmt']=startCurrencyChange
        allResults['endCurrencyAmt']=endCurrencyChange
        self.log_it('info', 'Start Currency Amt: {}, End Currency Amt: {}'.format(startCurrencyChange, endCurrencyChange))

        self.log_it('info', '==========END LARGE SMART ORDER==========')
        return allResults