import time


class Fees:
    def __init__(self, maker=.0025, taker=.0025):
        self.maker = maker
        self.taker = taker
        self.last_updated = time.time()

    def get_json(self):
    	return self.__dict__