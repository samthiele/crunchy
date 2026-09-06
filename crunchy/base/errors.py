"""
Error handling functions.
"""
from crunchy import log_error

# error loggers
def logAndStop( logDict, E, *, function, data, outpath, settings ):
    log_error("Stopping due to error in function %s" % function.__name__, E, logDict )
    return False
def logAndContinue( logDict, E, *, function, data, outpath, settings ):
    log_error("Continuing after error in function %s" % function.__name__, E, logDict )
    return True
