"""
Error handling functions.
"""
import traceback
from crunchy import log
from crunchy import getLogDict

# error loggers
def logAndStop( logDict, E, *, function, data, outpath, settings ):
    err = "EXCEPTION TRACE  PRINT:\n{}".format( "".join(traceback.format_exception(type(E), E, E.__traceback__)))
    log("Stopping due to error in function %s:\n %s\n" % (function.__name__, err ), logDict )
    return False
def logAndContinue( logDict, E, *, function, data, outpath, settings ):
    err = "EXCEPTION TRACE  PRINT:\n{}".format("".join(traceback.format_exception(type(E), E, E.__traceback__)))
    log("Continuing after error in function %s:\n %s\n" % (function.__name__, err ), logDict )
    return True