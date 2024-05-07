"""
Define function decorators that build entry points for crunchy workflows.
"""
import os
from pathlib import Path

import crunchy
from crunchy import getQueue
from crunchy import block
from crunchy import entries, setup, finalize
from crunchy import log, getLogDict
from crunchy.base.errors import logAndStop

# options that can be returned by file filters
ERROR = -1
REJECT = 0
WAIT = 1
PROCESS = 2

def init( func ):
    """
    Functions flagged by this decorator should take three arguments (input and output directories and a settings
    dictionary), and will be called when crunchy starts. This allows a workflow to setup e.g. file triggers, build
    an output structure or (in the case of the DummyWorkflow) create test data to operate on.

    Note that this will block the main thread until it has executed, so should not do anything too computational.

    If this function returns True or None then the workflow will be started after initialisation. If it returns False
    then an error will be thrown and the workflow will not start.

    :param func:
    :return:
    """

    # store reference to function
    setup[func.__name__] = func
    return func

def finish( func ):
    """
    Functions flagged by this decorator will be run (in the control thread) on finishing all jobs (i.e. when finish is
    clicked and all jobs have completed).

    :param func:
    :return:
    """
    # store reference to function
    finalize[func.__name__] = func
    return func

def fileTrigger(*, flow, fail=logAndStop, block=False, vb=1):
    """
    Makes a function a trigger point for a crunchy workflow and registers it with crunchy.

    :param flow: A list containing all workflow functions to be exectued (in order) on success of this filter.
    :param fail: An error handling functions if this or any subsequent workflow function fails.
    :param block: True if this workflow should be run in the current thread (blocking it). Default is False.
    :param vb: A number from 0 to 3 defining the amount of outputs to the log. Higher numbers give more outputs. Default is 1.
    :return: A filefilter decorator for wrapping around the underlying filter.
    """
    def decorator(func):
        func.flow = flow  # store workflow functions
        func.fail = fail  # what do do if it fails
        func.block = block
        func.log = getLogDict
        func.queue = getQueue
        def wrapper(path, outpath, settings):
            if vb >= 3:
                log("Filtering %s with %s" % (path, func.__name__), func.log() )

            # run source function to evaluate if file is valid for this workflow
            data = dict(path=Path(path))
            #print(path, os.path.exists(path))
            status = ERROR
            try:
                status, outpath = func(data, Path(outpath), settings)
            except BaseException as E:
                # pass everything to error handler
                func.fail( func.log(), E, function=func,
                             data=data,
                             outpath=outpath,
                             settings=settings)

            # the file filter ran and says we should process this file - add to queue!
            if status == PROCESS:
                os.makedirs(outpath, exist_ok=True)  # make sure outpath exists!
                if block:  # run job in this thread (block == True)
                    if vb >= 2:
                        log("Executing and blocking: %s" % path, func.log() )
                    crunchy.prog[path] = 1  # register job as running
                    _job(func.flow, func.fail, func.log(),
                         data, outpath, settings )
                    crunchy.prog[path] = 2  # register job as complete
                else:  # pass job to queue (block == False )
                    if vb >= 2:
                        log("Queuing job for: %s" % path, func.log())
                    crunchy.prog[path] = 0 # register job as queued.
                    func.queue().put( ("EXEC", (_job, (func.flow, func.fail, func.log(),
                                                      data, outpath, settings ), {})))
            return status

        # register function with crunchy
        entries[func.__name__] = wrapper
        return wrapper
    return decorator

# function for executing jobs in queue
def _job(flow, fail, logdict, data, outpath, settings):
    for f in flow:
        try:
            log("Running %s on %s"%(f.__name__, data['path']), logdict)
            f(data, outpath, settings)
        except BaseException as E:
            if not fail(logdict, E, function=f, data=data, outpath=outpath, settings=settings):
                return