# OEMS Manual

## Overview

## Setup OMS Server
### AWS
1. ...
### Environment Variables
Environment file located in ~/dev/crypto_oms
1. <EXCHANGE-NAME>_TRADE_KEYS
    -these keys will be used to authenticate with the exchange for trading
    -<PUBLIC KEY>,<PRIVATE KEY>
2. OMS_KEYS
    -this info will be used for OMS authentication
    -<USER NAME>,<PUBLIC KEY>
3. COIN_API_KEY
    -this key will be used to authenticate with the data provider
    -<PRIVATE KEY>
4. COIN_API_WS_ENDPOINTS
    -these end points will be used by the OMS to get market data
    -<WS END PT0>,<WS END PT1>
5. SENDER_EMAIL
    -email credentials used to send out notifications regarding a failed query
    -<EMAIL>,<PASS>
6. RECEIVE_EMAILS
    -emails which will receive notifications described above
    -<EMAIL0>,<EMAIL1>
7. DB_URL
    -URL pertaining to DB OMS will store query info in
    -postgres://<USER>:<PASS>@<HOST>:<PORT>/<DB NAME>

## Setup OMS Client
### Instalation
'''
git clone https://github.com/kennethgoodman/crypto_oms_2.0
sudo apt install gcc python-dev libgmp3-dev
pip install -r requirements.txt
'''
### Environment Variables
1. OMS_KEY
    -this info will be used for OMS authentication
    -<PRIVATE KEY>

## Quick Start
1. Create a file test_run.py in the same directory in which the client repo was cloned.
'''
from oms_client import OMSClient

userName = 'kenneth'
url = 'http://<OMS SERVER IP>:8888'
botName = 'DemoBot'
recordInDb = True
sendFailEmail = True
omsClient = OMSClient(userName, url, botName, recordInDb, sendFailEmail)

exchange = 'bittrex'
startCurrency = 'USDT'
endCurrency = 'ZEN'
startCurrencyAmount = 1000
smartOrderData = omsClient.smart_order(exchange,startCurrency,endCurrency,startCurrencyAmount)

TODO FINISH DEMO
'''
2. Start OMS Server
On the OMS host navigate to ~/dev/crypto_oms
'''
source ~/dev/omsenv/bin/activate
python app.py
'''
3. Run test_run.py
'''
python test_run.py
'''

## Manual
### Overview
1. valid exchange names
    - 'bittrex', 'kraken', 'gdax', 'binance', 'gemini'
2. valid side names
    - 'buy', 'sell'
3. Errors
    TODO point user to ccxt
4. Market Names
    TODO point user to ccxt
5. EMS Description
EMS
-order batch concept
	-orders are placed sequentially
	-time it takes to place orders (~.1 sec)
	-while orders are being placed, other orders which have already been placed get a Nchance to be filled
	-once all orders are placed, the first order which was placed is checked to see if its filled etc.
	-orders which are filled are put in 'inactive' mode
	-orders which weren't entirely filled are canceled and the amount remaining is replaced at a price closer to bid/ask
	-after a specified # of tries cancelling/replacing orders EMS will become a 'taker' placing orders 5bp below bid/ above ask

-check order status/cancel/replace
	-check order statis (different for each exchange ie: usally order id is all thats necessary, sometimes market symbol and order id, sometimes time, market symbol and order id)
	-every exchange will provide accurate info reguarding quantity of order filled vs remaining
		-sometimes cost/fees of order are inaccurate (show example of reconstructing order from trades)
	-edge case handled if an order is filled say hundeths of a second after checking its status
		-necessary to catch errors resulting from cancelling an order allready filled

-orders slightly too large/ too small
	-for some coins on Bittrex/Binance its not possible to sell all holdings
		-can sell .99999
	-same issue is taken care of for buying
		-if you have 100k USDT on bittrex, price of ADA is 0.22563316 and you want to buy 100k/0.22563316 = 443197 ADA
		-OMS will buy as much as possible without running into insufficient funds errors in a time efficient manner (in this case ~60sec)

-min order size
	-each exchange throws different errors and these erros usually can be ignored
		-Bittrex throws an error for buying too little/ selling too little/ and buying/selling an extremely small amount ie: < 1 Satoshi
		-Kraken/Binance has same errore for buying/selling

### Basic Functions
    #### market_order
        -Input
            -exchange
                - desc: name of exchange
                - type: str
            -side
                - desc: name of side
                - type: str
            -marketSym
                - desc: name of market
                - type: str
            -amount
                - desc: order amount
                - type: float
        - Behavior
            TODO breif description
        - Output
            TODO show example json
    #### limit_order
    TODO add these
    get_balances,
    get_past_orders,
    get_withdrawal_deposit_history,
    get_order_stats,
    get_fees, check_health, cancel_order, cance_all_orders

### Advanced Functions
    #### smart_order
    #### liquidate
    #### rebalance
