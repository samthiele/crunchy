import sys,os
from pathlib import Path
# from multiprocessing import Process, Queue, Manager, Value
from multiprocess import Process, Queue, Manager, Value
import time
from datetime import datetime

####################################################################################
### GLOBAL SETTINGS (allows easy editing / visualisation by apps)
### entries here will be exposed by the crunchy GUI.
####################################################################################
crunchy_settings = dict(
    inpath = dict(type='path', value='CrunchyIn', mustexist=True ),
    outpath = dict(type='path', value='CrunchyOut', mustexist=False),
    nthreads = dict(type='int', value=2, min=1, max=os.cpu_count()-1),
    wait = dict(type='int', value=5, min=1, max=100),
    idle = dict(type='float', value=1.0, min=0.0, max=100.0),
)
workflow_settings = { } # settings that are exposed to the GUI by the workflow for customisable settings.
dashboard = None # html template that is exposed to the GUI by the workflow for visualisation.

# once resolved, this will be a dictionary containing the settings used by crunchy.
settings = None

# these public variables all control message passing and the worker threads
block = None
endwhenempty = None
queue = None
manager = None
workers = None
setup = {} # links to workflow setup functions
entries = {} # links to workflow file filters
finalize = {} # links to workflow finish functions
scoutdirs={}
scoutthread = None
crunchthread = None
prog = None # dict storing a list of files being crunched. True if these have been passed to worker, False if they are queued.
logdict = None
file_size_dict = None
known_files = None
new_files = None

debug = True # true if log should be printed straight to console. N.B this is not shared across threads!

###################################################################################
## Getters: used for getting the above attributes. These are needed by decorators.
###################################################################################
def getQueue():
    return queue

def getLogDict():
    return logdict

def getProgress( status=0 ):
    """
    Return a list containing the file paths at the specified status.

    :param status: the file status to return. 0 is queued, 1 is running, 2 is completed.
    :return: A list of file paths.
    """
    out = []
    for k,v in prog.items():
        if v == status:
            out.append(k)
    return out

def printLog():
    for k,v in logdict.items():
        print("Thread %s" % k)
        print(v)

def log( message, logdict, master=False ):
    """
    Add a message to the log for the thread (based on PID) calling this function.
    :param message:  The message to add to the log.
    :param logdict: The (threadsafe) logging dictionary to log to. Use crunchy.getLogDict().
    :param master: True if this message should be added to the master log rather than the worker thread log.
                    N.B. master logging may not be threadsafe, so avoid calling often!
    """
    assert logdict is not None, "[%d] Error - log must not be none. Some multithreading nightmare has occured." % os.getpid()

    # get PID and time
    pid = os.getpid()
    time = datetime.now().strftime("%H:%M:%S")

    # init logging if need be
    if 'master' not in logdict:
        logdict['master'] = "[%s][%d] Logging started.\n"%(time,pid)
    if pid not in logdict:
        logdict[pid] = "[%s] Logging started.\n"%time

    # append message
    if master: # log to master log
        logdict['master'] += "[%s][%d] %s.\n" % (time, pid, message)
    logdict[pid] += "[%s] %s.\n"%(time,message) # also add to thread-specific log for reference

    if debug:
        print("[%s][%d] %s.\n"%(time,pid,message), end='') # also print straight to console

#################################################################
## Workers: the following setup, run and manage worker threads
#################################################################
def crunch(_queue, _logdict, _block, _end, _prog, _settings):
    """Read from the queue and process or execute corresponding messages"""
    
    # store globals in this thread
    global logdict
    global queue
    global block
    global end
    global prog
    global settings
    
    queue = _queue
    logdict = _logdict
    block = _block
    end = _end
    prog = _prog
    settings = _settings
        
    log("Worker initialised", logdict)
    
    while True:
        if bool( block.value ) is False:
            try:
                msg, value = queue.get(False, 0.1)
            except: # empty queue
                if bool( end.value ) is True:
                    break # done!
                else: # wait for further jobs
                    continue
            if msg == "EXEC":
                f,args,kwargs = value
                p = args[-3].get('path', "UNKNOWN") # get file path for updating progress dictionary.
                prog[p] = 1 # set as in-progress.
                f(*args,**kwargs) # execute function
                prog[p] = 2 # set as complete.
            else:
                log("Error - %s is an invalid message." % msg, logdict)
    log("Work complete", logdict)

def init( nworkers=None ):
    """
    Setup crunchy workers. Must be run from a __main__ = True scope.
    :param nthreads: The number of threads for crunchy to use. This will spawn nthreads workers, with the final
                     thread used to run a file-scout.
    """
    # setup threadsafe globals
    global manager
    global queue
    global block
    global endwhenempty
    global workers
    global logdict
    global file_size_dict
    global known_files
    global new_files
    global prog
    global settings

    queue = Queue()
    block = Value('i', 0)
    endwhenempty = Value('i', 0)
    manager = Manager()
    logdict = manager.dict()
    prog = manager.dict()
    settings = manager.dict()
    
    
    # setup dictionaries used by file trackers
    file_size_dict = manager.dict()  # store file size info
    known_files = manager.dict()  # files we know about, so ignore
    new_files = manager.dict()  # dictionary containing new files (as keys)

    # populate default settings
    for k,v in workflow_settings.items():
        settings[k] = v['value']
    for k,v in crunchy_settings.items():
        settings[k] = v['value']
    
    # setup queue
    workers = []
    if nworkers is None:
        nworkers = settings['nthreads']
    for i in range(nworkers):
        # create worker process
        p = Process(target=crunch, args=(queue, logdict, block, endwhenempty, prog, settings))
        p.daemon = True # it should run in the background
        p.start() # start it
        workers.append(p) # store reference to it

def initialised():
    """
    Return True if crunchy has been inititalised and worker threads have spawned (and are awaiting or executing tasks).
    """
    return workers is not None

def running():
    """
    Return True if crunchy has been inititalised and a scout thread is running to pass jobs to worker threads.
    """
    return initialised() and scoutthread is not None

def pause():
    """
    Pause worker threads. Currently running jobs will be finished, but new jobs will not be launched
    from the queue. This will not pause file scouts.
    """
    global block
    block.value = 1

def paused():
    global block
    return block.value == 1

def resume():
    """
    Resume paused worker threads so that they continue taking new jobs from the queue.
    """
    global block
    block.value = 0

def wait():
    """
    Join each worker until the next time the queue becomes empty.
    """
    while not queue.empty():
        time.sleep(0.5)

def complete(join = True, end=True):
    """
    Tell all workers to finish once the queue is empty.

    :param join: True if this thread should block until all workers have completed. Default is True.
    :param end: True if all threads (including the file scout) should be shut down after work is finished. Default is True.
                N.B. if end is True and join is False then workers will be brutally slaughtered.
    """
    global endwhenempty
    global scoutthread
    global scoutdirs
    global crunchthread
    global workers
    global settings
    global finalize
    global logdict
    endwhenempty.value = 1 # tell jobs to exit once queue is empty
    if join:
        if workers is not None:
            for w in workers:
                w.join()
    if end:
        if workers is not None:
            for w in workers:
                if w.is_alive():
                    w.terminate()
        workers = None
        if scoutthread is not None:
            if scoutthread.is_alive():
                scoutthread.terminate()
                scoutthread = None
        if crunchthread is not None:
            if crunchthread.is_alive():
                crunchthread.terminate()
                crunchthread = None
        scoutdirs.clear()

    # and finally, run any "finish" jobs ( in this thread )
    for k, v in finalize.items():
        log('Running final tasks: [%s]' % k, logdict, master=True )
        v(Path(settings['inpath']), Path(settings['outpath']), settings)

#################################################################
## Scout: the following sets up and manages scout threads
#################################################################
def add( path, depth, clear=False ):
    """
    Add a "hot directory" that crunch passes to work filters.

    :param path: path to the hot directory to add
    :param depth: the search depth to look at files or folders within this directory.
    :param clear: remove any existing paths before adding new one. Default is False.
    """
    if clear:
        scoutdirs.clear()
    scoutdirs[ str(path)] = int(depth)

def scout( clear = True ):
    """
    Start crunchy scout, which will begin passing files to worker threads. If a scout has already been launched,
    then it will be terminated and a new one run (in case e.g. settings have changed). Note that this will not
    launch a thread passing identified files to the workflow - use crunchy.run() to do this.

    :param clear: True if the known_files, file_size_dict and new_files dictionaries should be emptied before launching
               or re-launching this scout. Default is True.
    """

    assert len(scoutdirs) > 0, "Error - cannot start crunchy without any hot directories."

    # terminate old scout if it is running
    global scoutthread
    if scoutthread is not None:
        scoutthread.terminate()
        scoutthread = None

    # clear file dictionaries
    if clear:
        new_files.clear()
        known_files.clear()
        file_size_dict.clear()

    # launch new scout thread
    from .base import scout # here to avoid circular imports
    scoutthread = Process(target=scout,
                     args=(list(scoutdirs.keys()),  # search paths
                           list(scoutdirs.values()),  # search depths
                           new_files,
                           known_files,
                           file_size_dict,
                           int(settings.get('wait',1)),
                           float(settings.get('idle',1.0))))
    scoutthread.start()
    return scoutthread

def setOutpath( path ):
    settings['outpath'] = Path(path)
    crunchy_settings['outpath']['value'] = Path(path)

def setInpath( path ):
    settings['inpath'] = Path(path)
    crunchy_settings['inpath']['value'] = Path(path)

def process_new_files( settings, new_files, known_files, entries, outpath, _logdict ):
    """
    Process any new files in the new_files dictionary and pass them to each listening filefilter to trigger
    workflow events.
    """
    assert settings.get('outpath',None) is not None, 'Error - no outpath set. See setOutpath()'
    os.makedirs(settings['outpath'], exist_ok=True)

    from crunchy.base import trigger # here to circular imports
    if new_files is not None:
        for p,s in new_files.items():
            known_files[p] = s # we can now ignore this file (unless the file size changes at some point)
            for f in entries.values():
                status = f(Path(p), Path(settings['outpath']), settings ) # pass to our file filters
                if status == trigger.WAIT: # file-filter wants to see this file again; remove it from known files
                    del known_files[p]
        new_files.clear()
    else:
        assert False, 'Error - a serious multithreading failure has occurred...'

# setup thread that runs crunchy (passes files to worker threads).
def _serve(_settings, _queue, _new_files, _known_files, _entries, _outpath, _prog, _logdict):
    """
    Private function called by thread spawned by (run).
    """
    
    # define globals in this thread
    global settings
    global queue
    global new_files
    global known_files
    global entries
    global outpath
    global prog
    global logdict
    
    settings = _settings
    queue = _queue
    new_files = _new_files
    known_files = _known_files
    entries = _entries
    outpath = _outpath
    prog = _prog
    logdict = _logdict
    
    import crunchy
    crunchy.log('Spawning control thread.', logdict)
    while True:
        #assert outpath is not None, 'Error - no outpath set. See setOutpath()'
        #for p, s in new_files.items():
        #    known_files[p] = s  # we can now ignore this file (unless the file size changes at some point)
        #    for f in entries.values():
        #        f(Path(p), Path(outpath), settings)  # pass to our file filters
        #new_files.clear()
        process_new_files(settings,new_files, known_files, entries, outpath, prog)
        time.sleep(1.0)
        
def run():
    """
    Run crunchy in a separate thread that loops continuously looking for new files and triggering events on the
    worker queue. Note that this will automatically launch a scout thread also by calling crunchy.scout().
    :return: The process crunchy is running in. This can be used to e.g., join() the main thread.
    """

    # populate settings
    for k,v in workflow_settings.items():
        settings[k] = v['value']
    for k,v in crunchy_settings.items():
        settings[k] = v['value']

    # run initialisation code in workflow
    # N.B. this is run in the master thread, so will block until all setup is complete.
    # N.B.B. this is intentional ;-)
    succ = True
    for k,v in setup.items():
        succ = succ & v( Path(settings['inpath']), Path(settings['outpath']), settings )
    if not succ:
        print("Crunchy workflow could not start due to errors during initialisation.")
        return None

    # start scout thread
    scout()

    #prog['keytest'] = 'value'

    # launch crunchy thread
    global crunchthread
    crunchthread = Process(target=_serve, args=(settings, queue, new_files, known_files, entries, settings['outpath'], prog, logdict))
    crunchthread.start()
    return crunchthread
    
