class InvalidExchangeQuery(Exception):
    def __init__(self, exchange, funcName=None):
        if(funcName is None):
            super(InvalidExchangeQuery, self).__init__("exchange {} is not supported".format(exchange))
        else:
            super(InvalidExchangeQuery, self).__init__("function {} is not supported for exchange(s) {}".format(funcName,exchange))

class VerificationError(Exception):
    def __init__(self, user, exchange, funcName):
        super(VerificationError, self).__init__("{} does not have permissions to use function {} on exchange {}".format(user,funcName,exchange))