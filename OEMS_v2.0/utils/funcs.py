import time
import ccxt
import urllib2
from socket import error as SocketError

def run_function_safe(f, *args, **kwargs):
    max_tries = kwargs.get('maxTries', 4)
    wait_time = kwargs.get('waitTime', 2)

    log = kwargs.get('log', None)

    result = None
    for tryNum in range(max_tries):
        try:                
            result = f(*args, **kwargs)
            break
        except (ccxt.RequestTimeout, ccxt.ExchangeNotAvailable, urllib2.HTTPError, ccxt.InvalidNonce, SocketError) as e:
            msg = 'Failed to run f: {}. There was an exception: {}'.format(f.__name__, e)
            if log is not None:
                log.error(msg)
            else:
                print(msg)
        except ccxt.ExchangeError as e:
            msg = 'Failed to run f: {}. There was an exception: {}'.format(f.__name__, e)
            #error specific to binance which can happen at any API call
            if('An unknown error occured while processing the request' in str(e) or
            'Internal error; unable to process your request. Please try again' in str(e) or
            'An unexpected response was received from the message bus. Execution status unknown' in str(e) or
            'Timestamp for this request is outside of the recvWindow' in str(e)):
                if log is not None:
                    log.error(msg)
                else:
                    print(msg)
            #error specific to bittrex which can happen at any API call
            if('APIKEY_INVALID' in str(e)):
                if log is not None:
                    log.error(msg)
                else:
                    print(msg)
            else:
                raise e

        time.sleep(wait_time)

    return result