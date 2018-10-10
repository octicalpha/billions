import requests
import time
from simplejson.scanner import JSONDecodeError
import json

def get_data_safe(url,max_tries=3,wait_time=2.5,previousError=None, fail_on_5xx = True, wait_time_5xx = None):
    """
    :param url: url to get
    :param max_tries: max times to try
    :param wait_time: time in seconds between tries
    :return: dict
    """
    if wait_time_5xx is None:
        wait_time_5xx =  wait_time
    if max_tries == 0:
        if isinstance(previousError,Exception):
            raise previousError
        else:
            raise Exception("There was a problem and we have no more tries left")
    try:
        result = requests.get(url,verify=True)
    except (requests.exceptions.ConnectionError,requests.exceptions.ChunkedEncodingError) as e:
        time.sleep(wait_time)
        return get_data_safe(url,max_tries=max_tries - 1,wait_time=wait_time,previousError=e)

    try:
        result.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Wait longer to give time to their server.
        if 500 <= result.status_code < 600:
            if fail_on_5xx:
                time.sleep(wait_time_5xx)
                return get_data_safe(url, max_tries=max_tries - 1, wait_time=wait_time, previousError=e, fail_on_5xx=fail_on_5xx, wait_time_5xx=wait_time_5xx)
            # Don't decrement max tries because we don't want to fail.
            else:
                time.sleep(wait_time_5xx)
                return get_data_safe(url, max_tries=max_tries, wait_time=wait_time, previousError=e, fail_on_5xx=fail_on_5xx, wait_time_5xx=wait_time_5xx)
        else:
            time.sleep(wait_time)
            return get_data_safe(url, max_tries=max_tries - 1, wait_time=wait_time,
                                 previousError=e, fail_on_5xx=fail_on_5xx,wait_time_5xx=wait_time_5xx)

    try:
        return result.json()
    except JSONDecodeError:
        try:
            return json.loads("{" + result.text + "}")
        except JSONDecodeError as jde:
            print("The status code was: {}".format(result.status_code))
            print("The raw message was: {}".format(result.raw.read()))
            print("The raw text was: {}".format(result.text))
            raise jde