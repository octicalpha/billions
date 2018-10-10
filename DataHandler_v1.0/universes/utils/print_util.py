from __future__ import print_function
import sys
import threading
import time
import logging

__flush_thread = None
__continueThreading = True

def eprint(*args, **kwargs):
    """
    Taken from https://stackoverflow.com/questions/5574702/how-to-print-to-stderr-in-python
    Wrapper around print to output to stderr
    :param args: args to print
    :param kwargs: kwargs to print
    :return: None
    """
    print(*args, file=sys.stderr, **kwargs)

def flush_output():
    sys.stderr.flush()
    sys.stdout.flush()

def flush_periodically(wait_time=15):
    while True and __continueThreading:
        flush_output()
        time.sleep(wait_time)

def flush_periodically_thread(wait_time=15):
    global __continueThreading
    __continueThreading = True
    __flush_thread = threading.Thread(target=flush_periodically,kwargs={'wait_time':wait_time})
    __flush_thread.daemon = True
    __flush_thread.start()

def stop_flush_thread():
    global __continueThreading
    __continueThreading = False

def _get_logger(file_name,level):
    logger = logging.getLogger(file_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s::[%(levelname)s]:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    ch = logging.FileHandler(file_name, mode='a')
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def get_info_logger(file_name):
    return _get_logger(file_name, logging.NOTSET)

def get_error_logger(file_name):
    return _get_logger(file_name, logging.ERROR)