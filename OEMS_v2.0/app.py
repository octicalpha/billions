from flask import Flask, request, jsonify
from flask.json import JSONEncoder
from flask_restful import reqparse, Api, Resource
from Exchanges import GDAX, Kraken, Gemini, Bittrex
from Exchanges.Binance import Binance
from multi_exchange_methods import MultiExchangeMethods
from oms_errors import InvalidExchangeQuery, VerificationError
from verify import verified
from collections import defaultdict
import json
import traceback

from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime
import ast
import pandas as pd

import logging

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


logger = logging.getLogger("oms_app.log")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='%(asctime)s::[%(levelname)s]:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
ch = logging.FileHandler("oms_app.log", mode='a')
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

load_dotenv(find_dotenv())
DB_URL = os.environ.get("DATABASE_URL_AWS_DEV")
DB_ENGINE = create_engine(DB_URL)


class JSONEncoderTimestamp(JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        elif (type(obj) == pd.Timestamp or type(obj) == datetime):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


app = Flask(__name__)
app.json_encoder = JSONEncoderTimestamp
api = Api(app)

exchanges = None
if os.path.isfile('binance_prod.key'):
    exchanges = {
        'bittrex': Bittrex('bittrex_prod.key'),
        'kraken': Kraken('kraken_prod.key'),
        'binance': Binance('binance_prod.key')
    }
else:
    exchanges = {
        'bittrex': Bittrex('bittrex_dev.key'),
        'kraken': Kraken('kraken_dev.key'),
        'binance': Binance('binance_dev.key')
    }
multiExchangeMethods = MultiExchangeMethods(exchanges)

args_dict = {
    'verify_args': ['name', 'r', 's', 'params', 'timestamp', 'botName', 'recordInDb', 'sendFailEmail']
}

exchange_to_valid_functs = {}
api_functions = {
    'trade': ['limit_order', 'smart_order', 'rebalance', 'liquidate', 'cancel_order', 'update_balances',
              'update_ls_balance_via_trades', 'update_cushion_balance_via_trades', 'cancel_open_orders', 'store_ls_balance',
              'store_borrow_balance', 'store_cushion_balance', 'store_reserve_currency_added', 'update_borrow_balance_via_wds'],
    'accountInfo': ['get_balances', 'get_order_stats',
                    'get_ls_positions_via_balances', 'get_ls_positions_via_db', 'get_past_trades',
                    'check_health', 'get_past_orders', 'get_withdrawal_deposit_history', 'get_last_ls_balance'
                    'get_last_borrow_balance', 'get_last_cushion_balance', 'get_last_reserve_currency_added',
                    'get_wds_since_last_borrow_balance_store']}
for exchangeName in exchanges:
    exchange_to_valid_functs[exchangeName]=api_functions
for exchangeName in exchanges:
    if(exchangeName == 'kraken' or exchangeName == 'binance'):
        exchange_to_valid_functs[exchangeName]['trade'].append('market_order')
    if(exchangeName == 'binance' or exchangeName == 'gdax'):
        exchange_to_valid_functs[exchangeName]['accountInfo'].append('get_real_order_info')
    if(exchangeName != 'gdax'):
        exchange_to_valid_functs[exchangeName]['accountInfo'].append('get_fees')
exchange_to_valid_functs['multiExchangeAPI'] = ['large_smart_order']

def log_action(params, call_type, exception=False, print_traceback=True):
    if exception:
        logger.exception(
            "Exception running {} {}".format(call_type, params))
        if print_traceback:
            traceback.print_exc()
    else:
        logger.info('calling {} {}'.format(call_type, params))


parser = reqparse.RequestParser()
for arg in args_dict['verify_args']:
    parser.add_argument(arg)


def store_invocation_data(botName, apiFuncName, data, exchange):
    data = pd.DataFrame(
        [{'result': json.dumps(data, cls=JSONEncoderTimestamp), 'in_z': datetime.utcnow(), 'exchange': exchange}])
    data.to_sql(apiFuncName, DB_ENGINE, if_exists='append', schema=botName, index=False)


def strings_to_datetime(params):
    for i, param in enumerate(params['args']):
        try:
            datetime.strptime(param, '%Y-%m-%d %H:%M:%S.%f')
            params['args'][i] = pd.Timestamp(param)
        except:
            try:
                datetime.strptime(param, '%Y-%m-%d %H:%M:%S')
                params['args'][i] = pd.Timestamp(param)
            except:
                pass

def email_on_fail(errorString):
    fromaddr = "altsimbot@gmail.com"
    toaddrs = ['tombotnotification@gmail.com', "exchanges@altcoinadvisors.com"]

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "altcoin1234")

    for toaddr in toaddrs:
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = "BOT DOWN"
        body = errorString
        msg.attach(MIMEText(body, 'plain'))
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
    server.quit()

def oms_abort(error, msg, sendFailEmail, traceBackString=None):
    if (sendFailEmail and traceBackString is None):
        email_on_fail(msg)
    elif(sendFailEmail and traceBackString is not None):
        email_on_fail(traceBackString)
    return {'successOMS':False,'errorType':error.__class__.__name__,'message':msg}

class MultiExchangeTrade(Resource):
    post_calls = defaultdict(list)

    def post(self):
        args = parser.parse_args()
        try:
            self.post_calls[request.environ['REMOTE_ADDR']].append(args)

            params = json.loads(args['params'])

            # check if api func/exchanges exist
            if(params['apiFunc'] not in exchange_to_valid_functs['multiExchangeAPI']):
                raise InvalidExchangeQuery(str(params['exchanges']), params['apiFunc'])

            if not verified(args):
                raise VerificationError(args['name'], params['exchanges'], params['apiFunc'])

            strings_to_datetime(params)

            returnValue = getattr(multiExchangeMethods, params['apiFunc'])(*params['args'])

            if (returnValue is None):
                returnValue = {'successOMS': True}
            else:
                returnValue = {'successOMS': True, 'result': returnValue}

            if (ast.literal_eval(args['recordInDb'])):
                store_invocation_data(args['botName'], params['apiFunc'], returnValue, 'multiExchange')

            log_action(params, "MultiExchangeTrade")

            return jsonify(returnValue)
        except Exception as e:
            log_action(params, "MultiExchangeTrade", exception=True, print_traceback=True)
            return oms_abort(e, str(e), ast.literal_eval(args['sendFailEmail']), traceback.format_exc())

class Trade(Resource):
    post_calls = defaultdict(list)
    def post(self):
        args = parser.parse_args()
        try:
            self.post_calls[request.environ['REMOTE_ADDR']].append(args)

            params = json.loads(args['params'])

            if(params['exchange'] not in exchanges or
                    params['apiFunc'] not in exchange_to_valid_functs[params['exchange']]['trade']):
                raise InvalidExchangeQuery(params['exchange'], params['apiFunc'])

            if not verified(args):
                raise VerificationError(args['name'], params['exchange'], params['apiFunc'])

            strings_to_datetime(params)

            returnValue = getattr(exchanges[params['exchange']], params['apiFunc'])(*params['args'])

            if(returnValue is None):
                returnValue = {'successOMS':True}
            else:
                returnValue = {'successOMS':True, 'result':returnValue}

            if (ast.literal_eval(args['recordInDb'])):
                store_invocation_data(args['botName'], params['apiFunc'], returnValue, params['exchange'])

            log_action(params, "Trade")

            return jsonify(returnValue)
        except Exception as e:
                log_action(params, "Trade", exception=True, print_traceback=True)
                return oms_abort(e, str(e), ast.literal_eval(args['sendFailEmail']), traceback.format_exc())


class AccountInfo(Resource):
    post_calls = defaultdict(list)

    def post(self):
        args = parser.parse_args()
        try:
            self.post_calls[request.environ['REMOTE_ADDR']].append(args)

            params = json.loads(args['params'])

            if (params['exchange'] not in exchanges or
                    params['apiFunc'] not in exchange_to_valid_functs[params['exchange']]['accountInfo']):
                raise InvalidExchangeQuery(params['exchange'], params['apiFunc'])

            if not verified(args):
                raise VerificationError(args['name'], params['exchange'], params['apiFunc'])

            strings_to_datetime(params)

            returnValue = getattr(exchanges[params['exchange']], params['apiFunc'])(*params['args'])

            if (params['apiFunc'] == 'get_fees'):
                returnValue = returnValue.get_json()

            if (returnValue is None):
                returnValue = {'successOMS': True}
            else:
                returnValue = {'successOMS': True, 'result': returnValue}

            returnValue['successOMS'] = True

            if (ast.literal_eval(args['recordInDb'])):
                store_invocation_data(args['botName'], params['apiFunc'], returnValue, params['exchange'])

            log_action(params, "AccountInfo")

            return jsonify(returnValue)
        except Exception as e:
            log_action(params, "AccountInfo", exception=True, print_traceback=True)
            return oms_abort(e, str(e), ast.literal_eval(args['sendFailEmail']), traceback.format_exc())

class Verify(Resource):
    def post(self):
        args = parser.parse_args()
        try:
            return jsonify({'verified': verified(args, logit=True), 'successOMS':True})
        except Exception as e:
            return oms_abort(e, str(e), ast.literal_eval(args['sendFailEmail']), traceback.format_exc())

api.add_resource(MultiExchangeTrade, '/multiexchangetrade')
api.add_resource(Trade, '/trade')
api.add_resource(AccountInfo, '/accountInfo')
api.add_resource(Verify, '/verify')

if __name__ == '__main__':
    if os.path.isfile('bittrex_prod.key'):
        app.run(host='0.0.0.0', port=8888)
    else:
        app.run(debug=True, port=8888)