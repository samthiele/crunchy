"""
Scouting thread that updates file maps for CrunchyServer (without blocking HTTP requests).
"""
import os
from time import sleep
import contextlib

def scout( paths, depth, new_files, known_files, file_size_dict,  wait = 5, idle=5.0, maxiter=None, stop=None, lock=None  ):
    """
    Monitor specified path at given depth.

    *Arguments*:
     - path = a list of directories to search in.
     - depth = a list containing the depth to search in each directory listed in path. If an integer, then a constant
               value is used for all files.
     - new_files = list to append newly found files to.
     - known_files = dictionary of known files (known_files[p] = True if a file should be ignored).
     - file_size_dict = dictionary to store visited file size in (file_size_dict[p] = [size1, size2, size3 ... ]
     - wait = the number of times to touch a file before it's filesize is considered to be "stable".
     - idle = time to sleep between iterations (full scans of all paths). Default is 5 seconds.
     - maxiter = the maximum number of iterations to run. Default is None (run forever).
     - stop = optional threading.Event; when set, the scout loop exits.
     - lock = optional lock guarding new_files / known_files / file_size_dict.
    """
    i = 0
    if isinstance(depth, int) or isinstance(depth, float):
        depth = [depth] * len(paths)
    while maxiter is None or i < maxiter:
        if stop is not None and stop.is_set():
            break
        for p,d in zip(paths, depth):
            if stop is not None and stop.is_set():
                break
            if p == '' or p is None:
                continue # allow input path or output path to be None (e.g. if we only want to run analyses)

            files = _checkNewFiles( p, d, wait, known_files, file_size_dict, lock ) # check for new files in path
            ctx = lock if lock is not None else contextlib.nullcontext()
            with ctx:
                for f,s in files:
                    new_files[f] = s # store new file and its size
        sleep(idle)
        i+=1

def _checkNewFiles(path, level, wait, known_files, file_size_dict, lock=None):
    """
    Search the specified directory at the specified depth and return a list of
    files that (1) have a stable file size, and (2) are not in self.known_files.

    *Arguments*:
     - path = the path to check.
     - level = the level to recurse down to before checking files. Default is 0 (search current dir).
     - wait = how many iterations to wait before deciding file size has stabilised.
     - known_files = a dictionary with known_files[ path ] = True if paths should be ignored.
     - file_size_dict = dictionary with file_size_dict[ path ] = [ size1, size2, size3 ... ].
     - lock = optional lock guarding the file dictionaries.
    *Returns*:
     - a list of newly found files that have stable file size.
    """
    os.makedirs(path, exist_ok=True)  # scout dir must exist.
    if level > 0:
        # move down a level and recurse
        out = []
        for f in os.scandir(path):
            if f.is_dir(follow_symlinks=False):
                out += _checkNewFiles(f.path, level - 1, wait, known_files, file_size_dict, lock)
        return out
    else:
        # search directory and update file size dictionary
        out = []
        ctx = lock if lock is not None else contextlib.nullcontext()
        for f in os.scandir(path):
            p = f.path
            fsize = _getFileSize(p, entry=f)
            with ctx:
                if p in known_files:
                    try:
                        if known_files[p] != fsize:
                            del known_files[p] # remove from known files as size has changed
                    except KeyError:
                        print("Warning: key error for %s" % p )
                        pass # concurrent delete from the control thread
                if p not in known_files:
                    if p not in file_size_dict:  # this is a completely new file
                        file_size_dict[p] = (fsize, 1) # size, visited once
                    else:  # track this files file size
                        old_size, visits = file_size_dict[p]
                        if fsize == old_size: # size hasn't changed!
                            file_size_dict[p] = (fsize, visits+1)

                            # have we visited this enough times to consider it stable?
                            if visits >= wait:  # yes! Return it
                                out.append( (p, fsize ) )
                                del file_size_dict[p]  # cleanup
                        else: # file size has changed; reset visit count
                            file_size_dict[p] = (fsize, 1)
        return out

def _getFileSize(path, entry=None):
    """
    This will get the full size of a file or directory (including subdirs).
    """
    if entry is not None:
        try:
            if entry.is_file(follow_symlinks=False):
                return entry.stat(follow_symlinks=False).st_size
        except OSError:
            return 0
    total_size = 0
    if os.path.isdir(path):
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):  # skip if it is symbolic link
                    try:
                        total_size += os.path.getsize(fp)
                    except OSError:
                        pass
    elif os.path.isfile(path):
        try:
            total_size += os.path.getsize(path)
        except OSError:
            pass

    return total_size
