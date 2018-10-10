import time
import datetime

TIME_FORMAT = "%Y%m%dT%H%M%S"

def getCurrentDateAndTime():
    return time.strftime(TIME_FORMAT)

def getInfinityTime():
    return datetime.datetime(year=2099,month=1,day=1)