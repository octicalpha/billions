from collections import defaultdict
from copy import copy, deepcopy
import random
import numpy as np

class RouteFinder:
    """
    Finds buy/sell routes for exchanges with multiple quote currencies.

    Note:
    assume quote currency = currency you're buying with
    assume base currency = currency you're buying
    """
    def __init__(self, exchangeName, marketNames):
        #A market name should be formated Quote-Base
        self.quoteToBase = defaultdict(list)
        self.baseToQuote = defaultdict(list)
        self.exchangeName = exchangeName
        for marketName in marketNames:
            base,quote = marketName.split('/')
            self.quoteToBase[quote].append(base)
            self.baseToQuote[base].append(quote)
        self.allQuotes = self.quoteToBase.keys()

    def find_routes_recurse(self, startCurrency, endCurrency, quotesUsed):
        allPaths = []
        
        if(startCurrency in self.allQuotes):
            quotesUsed.append(startCurrency)

        if(startCurrency in self.quoteToBase and endCurrency in self.quoteToBase[startCurrency]):
            allPaths.append([startCurrency+'-'+endCurrency+'_BUY'])

        if(startCurrency in self.baseToQuote and endCurrency in self.baseToQuote[startCurrency]):
            allPaths.append([endCurrency+'-'+startCurrency+'_SELL'])

        if(startCurrency in self.quoteToBase):
            for base in self.quoteToBase[startCurrency]:
                if(base in self.allQuotes and base not in quotesUsed and base != endCurrency):
                    allPathsFromBase = self.find_routes_recurse(base, endCurrency, copy(quotesUsed))
                    for path in allPathsFromBase:
                        allPaths.append([startCurrency+'-'+base+'_BUY']+path)
        if(startCurrency in self.baseToQuote):
            for quote in self.baseToQuote[startCurrency]:
                if(quote not in quotesUsed and quote != endCurrency):
                    allPathsFromQuote = self.find_routes_recurse(quote, endCurrency, copy(quotesUsed))
                    for path in allPathsFromQuote:
                        allPaths.append([quote+'-'+startCurrency+'_SELL']+path)

        return allPaths

    def find_routes(self,startCurrency,endCurrency):
        if(startCurrency == endCurrency):
            raise NameError('The route finder requires start currency and end currency to be different. Currently they are both {}.'.format(startCurrency))
        
        routes = self.find_routes_recurse(startCurrency,endCurrency,[])
        #add exchange name to route symbol
        for route in routes:
            for i,marketAction in enumerate(route):
                route[i] = self.exchangeName+'_'+marketAction
        return routes

    def find_shortest_route(self, startCurrency, endCurrency, availableMarkets=None):
        allRouts = self.find_routes(startCurrency,endCurrency)
        if (availableMarkets is not None):
            availableRoutes = []
            for route in allRouts:
                available = True
                for action in route:
                    _,b,q,_=self.split_market_action(action)
                    if('{}/{}'.format(b,q) not in availableMarkets):
                        available = False
                if(available):
                    availableRoutes.append(route)
        else:
            availableRoutes = allRouts

        allRoutLens = list(map(lambda x: len(x), availableRoutes))

        if(not len(availableRoutes)):
            raise NameError('No route between {} and {}.'.format(startCurrency, endCurrency))

        return availableRoutes[allRoutLens.index(min(allRoutLens))]

    def uses_quote_market(self, route, quote):
        for market in route:
            if(market.split('-')[0].split('_')[1]==quote):
                return True
        return False
    
    def get_route_chain(self, route):
        chain = []
        _,b,q,s=self.split_market_action(route[0])
        if(s=='BUY'):
            chain.append(q)
            chain.append(b)
        else:
            chain.append(b)
            chain.append(q)
        for i,marketAction in enumerate(route[1:]):
            _,b,q,s=self.split_market_action(marketAction)
            if(s=='BUY'):
                chain.append(b)
            else:
                chain.append(q)
        return chain
        
    def split_market_action(self, marketNameOrderType):
        exchange, marketName,side = marketNameOrderType.split('_')
        quote,base = marketName.split('-')
        return exchange,base,quote,side

    def route_to_string(self, route):
        routeString = ''
        for i,action in enumerate(route):
            routeString+=action
            if(i < len(route)-1):
                routeString+='-->'
        return routeString

    def rout_to_market_name(self, route):
        e,b1,q1,s1=self.split_market_action(route[0])
        _,b2,q2,s2=self.split_market_action(route[-1])
        
        if(s1=='BUY' and s2=='BUY'):
            base = b2
            quote = q1
        elif(s1=='BUY' and s2=='SELL'):
            base = q1
            quote = q2
        elif(s1=='SELL' and s2=='BUY'):
            base = b2
            quote = b1
        else:#sell, sell
            base = b1
            quote = q2
            
        return '{}_{}_{}'.format(e,base,quote)
        
    def get_end_per_unit_start_currency(self, route, tickerDict):        
        rate = 1
        for marketNameOrderType in route:
            _,base,quote,side = self.split_market_action(marketNameOrderType)
            marketName = '{}/{}'.format(base, quote)
            if(side=='BUY'):
                rate*=(1/tickerDict[marketName]['last'])
            else:
                rate*=tickerDict[marketName]['last']

        return rate

    def convert_multiple_unit_portfolio_to_single_unit_portfolio(self, balances, currencyUnitSym, tickers):
        newBalances = {}
        for coin_or_market in balances:
            if('/' in coin_or_market):
                coin = coin_or_market.split('/')[0]
            else:
                coin = coin_or_market

            if(coin != currencyUnitSym):
                shortestRout = self.find_shortest_route(coin, currencyUnitSym, tickers.keys())
                commonCurrency_per_coin = self.get_end_per_unit_start_currency(shortestRout, tickers)
                newBalances[coin_or_market] = balances[coin_or_market]*commonCurrency_per_coin
            else:
                newBalances[coin_or_market] = balances[coin_or_market]

        return newBalances

    def convert_single_unit_portfolio_to_multiple_unit_portfolio(self, balances, currencyUnitSym, tickers):
        newBalances = {}
        for coin_or_market in balances:
            if('/' in coin_or_market):
                coin = coin_or_market.split('/')[0]
            else:
                coin = coin_or_market
            
            if(coin != currencyUnitSym):
                shortestRout = self.find_shortest_route(currencyUnitSym, coin, tickers.keys())
                commonCurrency_per_coin = self.get_end_per_unit_start_currency(shortestRout, tickers)
                newBalances[coin_or_market] = balances[coin_or_market]*commonCurrency_per_coin
            else:
                newBalances[coin_or_market] = balances[coin_or_market]

        return newBalances

    def convert_start_amoutn_to_end_amount(self, startCoin, endCoin, startAmount, tickers):
        route = self.find_shortest_route(startCoin, endCoin)
        endPerUnitStart = self.get_end_per_unit_start_currency(route, tickers)
        return startAmount*endPerUnitStart

    def route_to_side(self, route):
        if(len(route)>2):
            raise NameError('Route too long to handle. Route: {}'.format(route))

        _, _, _, side0 = self.split_market_action(route[0])
        _, _, _, side1 = self.split_market_action(route[1])
        #buy,buy = buy
        if(side0=='BUY' and side1=='BUY'):
            return 'BUY'
        elif(side0=='SELL' and side1=='SELL'):
            return 'SELL'
        else:
            chain = self.get_route_chain(route)
            sc = chain[0]
            ec = chain[-1]
            shortestRoute = self.find_shortest_route(sc, ec)
            if(len(shortestRoute)>1):
                raise NameError('Shortest route too long to handle. Route: {}'.format(shortestRoute))
            _,_,_,side = self.split_market_action(shortestRoute[0])
            return side


class MultiExchangeRouteFinder:
    def rout_to_market_name(self, route):
        e, b1, q1, s1 = self.split_market_action(route[0])
        _, b2, q2, s2 = self.split_market_action(route[-1])

        if (s1 == 'BUY' and s2 == 'BUY'):
            base = b2
            quote = q1
        elif (s1 == 'BUY' and s2 == 'SELL'):
            base = q1
            quote = q2
        elif (s1 == 'SELL' and s2 == 'BUY'):
            base = b2
            quote = b1
        else:  # sell, sell
            base = b1
            quote = q2

        return '{}_{}_{}'.format(e, base, quote)
    
    def get_recognizable_price(self, price, route):
        _,_,_,s = self.split_market_action(route[-1])
        if(s=='BUY'):
            return 1/price
        else:
            return price

    def get_recognizable_quantity(self, routeQuantityStart, routeQuantityEnd, route):
        _, _, _, s = self.split_market_action(route[-1])
        if (s == 'BUY'):
            return routeQuantityEnd
        else:
            return routeQuantityStart
            
    def split_market_action(self, marketNameOrderType):
        exchange, marketName, side = marketNameOrderType.split('_')
        quote,base = marketName.split('-')
        return exchange,base,quote,side
    
    def convert_start_amount_to_end_amount_with_fees_and_spread(self, route, allOrderBooks, tradingFees):
        endCurrencyPerUnitStartCurrency = 1
        for orderBookName in route:
            exchange, _,_,side = self.split_market_action(orderBookName)
            if(side=='SELL'):
                rate = allOrderBooks[orderBookName].bids[0].price*(1-tradingFees[exchange]['taker'])
            else:
                rate = (1/allOrderBooks[orderBookName].asks[0].price)*(1-tradingFees[exchange]['taker'])
            endCurrencyPerUnitStartCurrency *= rate
        return endCurrencyPerUnitStartCurrency    
        
    def get_max_quantity_for_route(self, route, allOrderBooks, tradingFees):
        def route_to_start_end_amounts(marketAction,allOrderBooks,tradingFees):
            exchange,_,_,side = self.split_market_action(marketAction)
            ob = allOrderBooks[marketAction]
            fee = tradingFees[exchange]['taker']
            if(side == 'BUY'):
                startAmount = ob.asks[0].price*ob.asks[0].quantity
                endAmount = ob.asks[0].quantity*(1-fee)
            else:
                endAmount = ob.bids[0].price * ob.bids[0].quantity*(1-fee)
                startAmount = ob.bids[0].quantity

            return startAmount, endAmount

        actualStartAmount, actualEndAmount = route_to_start_end_amounts(route[0], allOrderBooks, tradingFees)

        for marketAction in route[1:]:
            sa, ea = route_to_start_end_amounts(marketAction, allOrderBooks, tradingFees)
            actualMid = min(actualEndAmount, sa)
            actualStartAmount = actualMid*(actualStartAmount/actualEndAmount)
            actualEndAmount = actualMid*(ea/sa)

        return actualStartAmount, actualEndAmount

    def fill_order(self, quantity, asks_or_bids):
        if(asks_or_bids[0].quantity < quantity*.99):
            print(asks_or_bids[0].quantity,  quantity * .99)
            raise  NameError('Buying/selling more than can be filled at this price!')
        elif(asks_or_bids[0].quantity*.99 < quantity):
            del asks_or_bids[0]
        else:
            asks_or_bids[0].quantity-=quantity

    def adjust_orderbooks(self, bestroute, routeQuantity, allOrderBooks, tradingFees):
        e,_,_,_=self.split_market_action(bestroute[0])
        for orderBookName in bestroute:
            _,_,_,s = self.split_market_action(orderBookName)
            if(s=='BUY'):
                order = allOrderBooks[orderBookName].asks[0]
                routeQuantity*=(1/order.price)
                self.fill_order(routeQuantity, allOrderBooks[orderBookName].asks)
            else:
                order = allOrderBooks[orderBookName].bids[0]
                rate = order.price
                self.fill_order(routeQuantity, allOrderBooks[orderBookName].bids)
                routeQuantity*=rate

            routeQuantity*=(1-tradingFees[e]['taker'])

    def close_routes_with_no_liquidity(self, allRouteInfo, allOrderBooks):
        for curRoute in allRouteInfo['allRoutes']:
            for orderBookName in curRoute:
                _,b,q,s=self.split_market_action(orderBookName)
                if(s=='BUY'):
                    bids_or_asks = allOrderBooks[orderBookName].asks
                else:
                    bids_or_asks = allOrderBooks[orderBookName].bids
                if(len(bids_or_asks)==0):
                    for i,route in enumerate(allRouteInfo['allRoutes']):
                        if(orderBookName in route):
                            allRouteInfo['routeIsOpen'][i] = False

                    onerouteOpen = False
                    for isOpen in allRouteInfo['routeIsOpen']:
                        onerouteOpen = onerouteOpen or isOpen
                    if(not(onerouteOpen)):
                        raise NameError('Not enough liquidity to execute order!')
    
    def remove_negligable_routes(self, allrouteInfo, startCurrencyQuantity):
        # if amount being sent through a rout is negligable don't use the rout
        percentThroughAllRoutes = [amount / startCurrencyQuantity for amount in allrouteInfo['amountPerRoute']]
        highestPercentThroughRoutIdx = percentThroughAllRoutes.index(max(percentThroughAllRoutes))
        for i, percentThroughRoute in enumerate(percentThroughAllRoutes):
            if (percentThroughRoute < .01):
                allrouteInfo['amountPerRoute'][highestPercentThroughRoutIdx] += allrouteInfo['amountPerRoute'][i]
                allrouteInfo['amountPerRoute'][i] = 0

    def get_quantity_per_route(self, startCurrencyQuantity, allOrderBooks, allroutes, tradingFees, maxSlippage):
        #create route info data structure
        amountPerroute = [0 for i in range(len(allroutes))]
        routeIsOpen = [True for i in range(len(allroutes))]
        allrouteInfo = {'allRoutes':allroutes,'routeIsOpen':routeIsOpen,'amountPerRoute':amountPerroute, 'combinedOrderBook':[]}

        #deep copy orderbooks (orders will be filled over course of function)
        allOrderBooks = deepcopy(allOrderBooks)

        totalQuantityStart = 0
        totalQuantityEnd = 0
        
        firstPrice = None
        for i in range(100000):
            #broken markets or if every order in an orderbook has been filled
            self.close_routes_with_no_liquidity(allrouteInfo, allOrderBooks)
            
            #get currency conversions for each route based off best buy/sell orders + trading fees
            conversions = []
            for i, route in enumerate(allrouteInfo['allRoutes']):
                if(allrouteInfo['routeIsOpen'][i]):
                    conversions.append(self.convert_start_amount_to_end_amount_with_fees_and_spread(route, allOrderBooks, tradingFees))
                else:
                    conversions.append(0)
            bestrouteIdx = conversions.index(max(conversions))
            bestroute = allrouteInfo['allRoutes'][bestrouteIdx]
            
            #get quantity that can be traded on this route at this optimal conversion rate
            routeQuantityStart,routeQuantityEnd = self.get_max_quantity_for_route(bestroute, allOrderBooks, tradingFees)
            
            if(firstPrice is None):
                firstPrice = max(conversions)
            elif(maxSlippage is not None and abs(max(conversions)-firstPrice)/firstPrice > maxSlippage):
                self.remove_negligable_routes(allrouteInfo, startCurrencyQuantity)
                return allrouteInfo, totalQuantityEnd
            
            
            allrouteInfo['combinedOrderBook'].append((self.rout_to_market_name(bestroute),
                                                 self.get_recognizable_price(max(conversions), bestroute),
                                                 self.get_recognizable_quantity(routeQuantityStart, routeQuantityEnd, bestroute)))
            
            #print('route Quantity: '+str(routeQuantity)+' Total Quantity: '+str(totalQuantity)+' Best route: '+str(bestroute))

            if(totalQuantityStart+routeQuantityStart>startCurrencyQuantity):
                allrouteInfo['amountPerRoute'][bestrouteIdx]+= (startCurrencyQuantity-totalQuantityStart)
                totalQuantityEnd+=(startCurrencyQuantity-totalQuantityStart)*(routeQuantityEnd/routeQuantityStart)
                break
            else:
                totalQuantityStart+=routeQuantityStart
                totalQuantityEnd+=routeQuantityEnd
                allrouteInfo['amountPerRoute'][bestrouteIdx]+=routeQuantityStart
            self.adjust_orderbooks(bestroute, routeQuantityStart, allOrderBooks, tradingFees)

        self.remove_negligable_routes(allrouteInfo, startCurrencyQuantity)
        
        return allrouteInfo, totalQuantityEnd