import pandas as pd
import time
from utils.postgresConnection import storeInDb
from utils.get_data_util import get_data_safe
from requests.exceptions import ChunkedEncodingError

def get_uri(end,coin=None):
    """
    :param end:
    :return:
    """
    assert end in ['coins','map','front','global','page','history',
                   'history/1day','history/7day','history/30day','history/90day','history/180day','history/365day']
    base = 'http://coincap.io/'
    if end in ['coins','map','front','global']:
        uri = base + end
    else: # end in 'history/XXX':
        uri = base + "{}/{}".format(end,coin)
    return uri

def get_data(end,coin=None):
    return get_data_safe(get_uri(end,coin=coin),fail_on_5xx=False,wait_time_5xx=10)

def get_coins():
    """
    :return: list of strings
    """
    return get_data('coins')

def get_map():
    """
    :return: list of dictionaries
    """
    return get_data('map')

def get_front():
    """
    :return: list of dictionaries
    """
    return get_data('front')

def get_global():
    """
    :return: list of one dictionary
    """
    return [get_data('global')]

def get_page(coin):
    return [get_data('page',coin=coin)]


def join_history_data(data):
    market_cap = pd.DataFrame(data['market_cap'], columns=['ts', 'marketcap'])
    price = pd.DataFrame(data['price'], columns=['ts', 'price'])
    volume = pd.DataFrame(data['volume'], columns=['ts', 'volume'])
    return market_cap.merge(price, on='ts', how='inner').merge(volume, on='ts', how='inner')

def get_history_of_coin(coin):
    return get_data('history',coin=coin)

def get_1_day_history_of_coin(coin):
    return get_data('history/1day',coin=coin)

def get_7_day_history_of_coin(coin):
    return get_data('history/7day',coin=coin)

def get_30_day_history_of_coin(coin):
    return get_data('history/30day',coin=coin)

def get_90_day_history_of_coin(coin):
    return get_data('history/90day',coin=coin)

def get_180_day_history_of_coin(coin):
    return get_data('history/180day',coin=coin)

def get_365_day_history_of_coin(coin):
    return get_data('history/365day',coin=coin)

def store_it(tablename,f):
    try:
        data = f()
        return storeInDb(pd.DataFrame(data), tablename, 'aws_dev', schema='coincap')
    except ChunkedEncodingError as CEE:
        print("There was a CEE: {}".format(CEE))

def continuously_store(tablename,f,waittime=2.5):
    while True:
        store_it(tablename,f)
        time.sleep(waittime)