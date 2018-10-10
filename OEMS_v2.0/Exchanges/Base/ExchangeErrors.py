class InvalidArguments(Exception):
    def __init__(self, message):
        super(InvalidArguments, self).__init__(message)