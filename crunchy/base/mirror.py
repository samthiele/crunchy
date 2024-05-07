from datetime import datetime
import os
from shutil import copy2
import time
import numpy as np

def mirror( local_path, remote_path, sleeptime=5.0, maxiter=np.inf, delete = False, debug=True ):
    """

    Mirrors a local directory with a remote one to facilitate data transfer from a local machine to remote
    processing server. This will block the thread it is called in indefinitely.

    *Arguments*:
     - local_path = the local directory to mirror. Will be created if necessary.
     - remote_path = the remote directory to mirror. Will be created if necessary.
     - maxiter = the maximum number of iterations to run mirror. Default is inf (run forever).
     - delete = True if files in remote but not in local should be deleted. Dangerous - use with care!! (Default is False).
     - sleeptime = the sleep time between checks on directory. Default is 10 seconds.
    """

    if debug:
        now = datetime.now()
        dt = now.strftime("%d/%m/%Y %H:%M:%S")
        print("[%s] Setup mirror:" % (dt))
        print("    Local: %s" % local_path )
        print("    Remote: %s" % remote_path )
       
    # init directory maps
    local = {} # key = path, value = size
    waiting = False # print "waiting..." when no files are copying
    i = 0
    while i < maxiter: # loop indefinitely

        # check in and out directories exist
        if not os.path.exists(local_path):
            os.makedirs(local_path) # create indir if necessary
        if not os.path.exists(remote_path):
            os.makedirs(remote_path) # create outdir if necessary

        # map local and remote directories
        L = _scrape_( local_path, local_path )
        R = _scrape_( remote_path, remote_path )

        # remove keys with unstable filesize
        for key, val in L.items():
            if key in local:
                if local[key] != L[key]: # file-size differ
                    del local[key] # remove this file for this iteration

        # sleep here to ensure that sleeptime is guaranteed
        time.sleep(sleeptime)
        
        # copy new / modified files
        for key, val in local.items():
            if (key in R) and R[key] == local[key]:
                continue # no need to copy
            else: # there is a file to copy
                waiting = False
                if debug:
                    now = datetime.now()
                    dt = now.strftime("%d/%m/%Y %H:%M:%S")
                    print("[%s] Copying new %s to remote." % (dt, key,))
                os.makedirs(os.path.dirname(os.path.join(remote_path, key )), exist_ok=True)
                copy2( os.path.join(local_path, key), os.path.join(remote_path, key ) ) # N.B. this will block until copying is complete!
                R[key] = local[key] # update

        # delete files in remote that are not in local
        if delete:
            for key, val in R.items():
                if key not in L:
                    if debug:
                        now = datetime.now()
                        dt = now.strftime("%d/%m/%Y %H:%M:%S")
                        print("[%s] Deleting %s from remote." % (dt, key ))
                    os.remove(os.path.join(remote_path, key) ) # remove file
                    if not os.listdir( os.path.dirname( os.path.join(remote_path, key) )): # empty directory? remove.
                        os.removedirs( os.path.dirname( os.path.join( remote_path, key ) ) )

        local = L # update local map
        
        if debug and not waiting:
            waiting = True
            print("Mirror complete. Waiting for new files...", end='\r')
        
        # sleep
        time.sleep(sleeptime)
        i += 1

def _scrape_( p, base ):
    """
    Add local files to dictionary and recurse on subdirectories.
    """
    out = {}
    for root, directories, files in os.walk(p):
        rel_root = os.path.relpath( root, base )
        if rel_root == '.': # avoid having '.' in paths
            rel_root = ''

        # add files
        for f in files:
            out[ os.path.join(rel_root, f)] = os.path.getsize( os.path.join(root, f) )

        # scrape subdirectories
        for d in directories:
            out = {**out, **_scrape_( os.path.join(root,d), base ) }
    return out
