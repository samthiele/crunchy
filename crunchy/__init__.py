import os
import json
import shutil
import tempfile
import threading
import contextlib
import configparser
from pathlib import Path
from queue import Empty, PriorityQueue as _ThreadPriorityQueue
from datetime import datetime
import time

from multiprocess import Process, Queue, Value, set_start_method, get_start_method
from multiprocess.managers import SyncManager

####################################################################################
### GLOBAL SETTINGS (allows easy editing / visualisation by apps)
### entries here will be exposed by the crunchy GUI.
####################################################################################
def _cpu_count():
    n = os.cpu_count()
    return n if n and n > 0 else 2

_cpus = _cpu_count()

crunchy_settings = dict(
    inpath = dict(type='path', value='CrunchyIn', mustexist=True ),
    outpath = dict(type='path', value='CrunchyOut', mustexist=False),
    nthreads = dict(type='int', value=min(2, max(1, _cpus - 1)), min=1, max=max(1, _cpus - 1)),
    wait = dict(type='int', value=5, min=1, max=100),
    idle = dict(type='float', value=1.0, min=0.0, max=100.0),
    ram_reserve = dict(type='float', value=4.0, min=0.0, max=1024.0,
                       desc='Pause new jobs if free RAM is below this many GB. 0 disables.'),
)
workflow_settings = { } # settings that are exposed to the GUI by the workflow for customisable settings.
dashboard = None # html template that is exposed to the GUI by the workflow for visualisation.

_crunchy_cfg_applied = False
_workflow_cfg_ids = set()


_WRAP_QUOTES = (
    ("'", "'"),
    ('"', '"'),
    ('\u2018', '\u2019'),
    ('\u201c', '\u201d'),
)


def _strip_wrapping_quotes(text):
    """Drop matching '…' / \"…\" (and curly) wraps people paste around paths."""
    text = str(text).strip()
    changed = True
    while text and changed:
        changed = False
        for left, right in _WRAP_QUOTES:
            if len(text) >= 2 and text[0] == left and text[-1] == right:
                text = text[1:-1].strip()
                changed = True
                break
    return text


def _coerce_setting(entry, raw):
    """Cast a config-file value using the setting schema's ``type``."""
    dtype = str(entry.get('type', 'string')).lower()
    if isinstance(raw, (int, float, bool, Path)) and dtype != 'string':
        if dtype == 'int':
            return int(raw)
        if dtype == 'float':
            return float(raw)
        if dtype == 'bool':
            return bool(raw)
        if dtype == 'path':
            return Path(raw).expanduser()
        return raw
    text = _strip_wrapping_quotes(raw)
    if dtype == 'int':
        return int(float(text))
    if dtype == 'float':
        return float(text)
    if dtype == 'bool':
        return text.lower() in ('1', 'true', 'yes', 'on')
    if dtype == 'path':
        return Path(text).expanduser()
    return text


def _flatten_config_json(data):
    out = {}
    if not isinstance(data, dict):
        return out
    for key, val in data.items():
        if key in ('crunchy', 'workflow') and isinstance(val, dict):
            out.update(val)
        else:
            out[key] = val
    return out


def _parse_config_file(path):
    path = Path(path)
    if path.suffix.lower() == '.json':
        return _flatten_config_json(json.loads(path.read_text(encoding='utf-8')))
    parser = configparser.ConfigParser(
        interpolation=None,
        inline_comment_prefixes=('#', ';'),
    )
    parser.optionxform = str  # keep VNIR / inpath as written
    parser.read(path, encoding='utf-8')
    out = {}
    if parser.defaults():
        out.update(dict(parser.defaults()))
    for section in parser.sections():
        for key, val in parser.items(section):
            out[key] = _strip_wrapping_quotes(val)
    return out


def read_ini(path, *schemas, missing_ok=False):
    """
    Read a ``.ini`` or ``.json`` file.

    All sections are flattened (later keys win). If one or more setting schemas
    are passed (``crunchy_settings``, ``workflow_settings``, or any dict of
    ``{key: {type, value, ...}}`` entries), matching keys are coerced and
    written to each schema's ``value``.

    :param path: Config file path.
    :param schemas: Optional settings dictionaries to update in-place.
    :param missing_ok: Return ``{}`` instead of raising if the file is absent.
    :return: Flattened key/value dict (values as stored in the file).
    """
    path = Path(path).expanduser()
    if not path.is_file():
        if missing_ok:
            return {}
        raise FileNotFoundError(path)
    cfg = _parse_config_file(path)
    for schema in schemas:
        _apply_to_schema(schema, cfg)
    return cfg


def _config_search_paths():
    """Package ini, then clone-root / cwd ini+json, then $CRUNCHY_CONFIG."""
    pkg = Path(__file__).resolve().parent
    root = pkg.parent
    cwd = Path.cwd()
    ordered = [
        pkg / 'crunchy.ini',
        pkg / 'crunchy.json',
        root / 'crunchy.ini',
        root / 'crunchy.json',
        cwd / 'crunchy.ini',
        cwd / 'crunchy.json',
    ]
    env = os.environ.get('CRUNCHY_CONFIG')
    if env:
        ordered.append(Path(env))
    seen = set()
    out = []
    for p in ordered:
        try:
            rp = p.resolve()
        except Exception:
            continue
        if rp in seen or not p.is_file():
            continue
        seen.add(rp)
        out.append(p)
    return out


def _apply_to_schema(schema, cfg):
    if not schema or not cfg:
        return
    by_lower = {str(k).lower(): k for k in schema}
    for key, raw in cfg.items():
        dest = key if key in schema else by_lower.get(str(key).lower())
        if dest is None:
            continue
        entry = schema[dest]
        if not isinstance(entry, dict) or 'value' not in entry:
            continue
        try:
            entry['value'] = _coerce_setting(entry, raw)
        except Exception:
            pass


def apply_config(path=None, force=False):
    """
    Load defaults from crunchy.ini / crunchy.json and apply them.

    Crunchy keys are applied once at import (so scripts can still override
    ``crunchy_settings`` afterwards). Workflow keys that match the file are
    applied once per ``workflow_settings`` dict (after the workflow is imported).

    :param path: Optional file to load instead of the search path.
    :param force: Re-apply even if this schema was already updated.
    :return: Merged key/value dict from the file(s).
    """
    global _crunchy_cfg_applied
    cfg = {}
    if path is not None:
        cfg.update(read_ini(path))
    else:
        for p in _config_search_paths():
            try:
                cfg.update(read_ini(p))
            except Exception:
                continue
    if force or not _crunchy_cfg_applied:
        _apply_to_schema(crunchy_settings, cfg)
        _crunchy_cfg_applied = True
    if workflow_settings and (force or id(workflow_settings) not in _workflow_cfg_ids):
        _apply_to_schema(workflow_settings, cfg)
        _workflow_cfg_ids.add(id(workflow_settings))
    return cfg

# once resolved, this will be a dictionary containing the settings used by crunchy.
settings = None

# these public variables all control message passing and the worker processes
block = None
endwhenempty = None
queue = None
_raw_queue = None
manager = None
workers = None
setup = {} # links to workflow setup functions
entries = {} # links to workflow file filters
finalize = {} # links to workflow finish functions
scoutdirs={}
scoutthread = None
crunchthread = None
scout_stop = None
control_stop = None
prog = None # dict storing file status: 0 queued, 1 running, 2 completed
logdict = None
log_queue = None
log_lock = threading.Lock()
log_drain_stop = None
log_drain_thread = None
file_size_dict = None
known_files = None
new_files = None
file_filter = None  # path -> [(filter_name, status), ...]
files_lock = None
_parent_pid = None
worker_jobs = None # pid -> path of the job a worker is running
workers_lock = threading.Lock()
worker_watch_stop = None
worker_watch_thread = None

debug = True # true if log should be printed straight to console. N.B this is not shared across processes!

# numeric-library thread caps applied in workers (and at init, for spawn children)
_BLAS_ENV = (
    'OMP_NUM_THREADS',
    'OPENBLAS_NUM_THREADS',
    'MKL_NUM_THREADS',
    'NUMEXPR_NUM_THREADS',
    'VECLIB_MAXIMUM_THREADS',
)


class _CrunchyManager(SyncManager):
    pass

_CrunchyManager.register('PriorityQueue', _ThreadPriorityQueue)


class PriorityJobQueue(object):
    """
    Process-safe job queue. Higher ``priority`` values are dequeued first.
    Equal priorities stay in FIFO order.
    """
    def __init__(self, pq):
        self._pq = pq
        self._seq = 0
        self._lock = threading.Lock()

    def put(self, item, block=True, timeout=None, priority=0):
        with self._lock:
            self._seq += 1
            seq = self._seq
        self._pq.put(((-int(priority), seq), item), block=block, timeout=timeout)

    def get(self, block=True, timeout=None):
        _key, item = self._pq.get(block=block, timeout=timeout)
        return item

    def empty(self):
        return self._pq.empty()

    def qsize(self):
        try:
            return self._pq.qsize()
        except NotImplementedError:
            return 0


def _limit_numeric_threads(n=1):
    """Pin OpenMP / BLAS thread pools so worker processes do not oversubscribe."""
    nstr = str(max(1, int(n)))
    for key in _BLAS_ENV:
        os.environ[key] = nstr
    try:
        import threadpoolctl
        threadpoolctl.threadpool_limits(limits=int(nstr))
    except Exception:
        pass


def _memory_bytes():
    """
    Return ``(available, total)`` physical RAM in bytes, or ``(None, None)``
    if the platform cannot be queried. No extra package is required.
    """
    try:
        import psutil
        vm = psutil.virtual_memory()
        return int(vm.available), int(vm.total)
    except Exception:
        pass

    # Linux
    try:
        info = {}
        with open('/proc/meminfo', 'r') as fh:
            for line in fh:
                if ':' not in line:
                    continue
                key, val = line.split(':', 1)
                info[key] = val.strip()
        def _kb(name):
            return int(info[name].split()[0]) * 1024
        total = _kb('MemTotal')
        avail = _kb('MemAvailable') if 'MemAvailable' in info else _kb('MemFree')
        return avail, total
    except Exception:
        pass

    # Windows
    try:
        import ctypes
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ('dwLength', ctypes.c_ulong),
                ('dwMemoryLoad', ctypes.c_ulong),
                ('ullTotalPhys', ctypes.c_ulonglong),
                ('ullAvailPhys', ctypes.c_ulonglong),
                ('ullTotalPageFile', ctypes.c_ulonglong),
                ('ullAvailPageFile', ctypes.c_ulonglong),
                ('ullTotalVirtual', ctypes.c_ulonglong),
                ('ullAvailVirtual', ctypes.c_ulonglong),
                ('ullAvailExtendedVirtual', ctypes.c_ulonglong),
            ]
        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return int(stat.ullAvailPhys), int(stat.ullTotalPhys)
    except Exception:
        pass

    # macOS / BSD
    try:
        import subprocess
        page = int(subprocess.check_output(['sysctl', '-n', 'hw.pagesize']))
        total = int(subprocess.check_output(['sysctl', '-n', 'hw.memsize']))
        out = subprocess.check_output(['vm_stat'], text=True)
        pages = {}
        for line in out.splitlines():
            if ':' not in line:
                continue
            key, val = line.split(':', 1)
            digits = val.strip().rstrip('.').replace(',', '')
            if digits.lstrip('-').isdigit():
                pages[key.strip()] = int(digits)
        avail_pages = (
            pages.get('Pages free', 0)
            + pages.get('Pages inactive', 0)
            + pages.get('Pages speculative', 0)
            + pages.get('Pages purgeable', 0)
        )
        return avail_pages * page, total
    except Exception:
        pass

    return None, None


_GB = 1024 ** 3
_DISK_WARN_GB = 10.0


def _disk_bytes(path):
    """Free and total bytes on the volume that holds ``path`` (or its parent)."""
    try:
        start = Path(path).expanduser()
    except Exception:
        return None, None
    for candidate in (start, start.parent, Path.cwd()):
        try:
            if candidate.exists():
                usage = shutil.disk_usage(candidate)
                return int(usage.free), int(usage.total)
        except OSError:
            continue
    return None, None


def _log_day():
    return datetime.now().strftime('%Y-%m-%d')


def logs_root():
    """Root folder for dated log directories (``logs/YYYY-MM-DD/``)."""
    src = settings
    try:
        if src is not None and src.get('log_dir'):
            return Path(src['log_dir']).expanduser()
    except Exception:
        pass
    candidates = [
        Path(__file__).resolve().parent.parent / 'logs',
        Path.cwd() / 'logs',
        Path(tempfile.gettempdir()) / 'crunchy-logs',
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except OSError:
            continue
    return candidates[-1]


def today_log_dir():
    """``logs/YYYY-MM-DD`` (created if needed)."""
    day = logs_root() / _log_day()
    day.mkdir(parents=True, exist_ok=True)
    return day


def session_log_path():
    """Daily ``log.txt`` (all thread log lines). Override with settings['session_log']."""
    src = settings
    try:
        if src is not None and src.get('session_log'):
            return Path(src['session_log']).expanduser()
    except Exception:
        pass
    return today_log_dir() / 'log.txt'


def _ram_ok(settings=None):
    """True if a new job may start. ``ram_reserve`` is free GB required; 0 (or unknown RAM) disables."""
    src = settings if settings is not None else globals().get('settings')
    try:
        reserve_gb = float(src.get('ram_reserve', 4.0))
    except Exception:
        reserve_gb = 4.0
    if reserve_gb <= 0:
        return True
    avail, _total = _memory_bytes()
    if avail is None:
        return True
    return float(avail) >= reserve_gb * _GB


def _cap_workers_for_ram(nworkers, settings=None):
    """Drop to a single worker when free RAM is already below ``ram_reserve`` GB."""
    nworkers = max(1, int(nworkers))
    if _ram_ok(settings):
        return nworkers
    return 1


###################################################################################
## Getters: used for getting the above attributes. These are needed by decorators.
###################################################################################
def getQueue():
    return queue

def getLogDict():
    _flush_log_queue()
    return logdict

def getProgress( status=0 ):
    """
    Return a list containing the file paths at the specified status.

    :param status: the file status to return. 0 is queued, 1 is running, 2 is completed.
    :return: A list of file paths.
    """
    if prog is None:
        return []
    out = []
    try:
        items = list(prog.items())
    except Exception:
        return []
    for k,v in items:
        if v == status:
            out.append(k)
    return out

def printLog():
    d = getLogDict()
    if d is None:
        return
    for k,v in d.items():
        print("Thread %s" % k)
        print(v)

def _append_session_log(line):
    """Append one line to today's log.txt (best effort)."""
    try:
        path = session_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        if not line.endswith('\n'):
            line = line + '\n'
        with open(path, 'a', encoding='utf-8') as fh:
            fh.write(line)
    except Exception:
        pass


def _write_log(d, pid, master, ts, message):
    """Append one line to the in-process log dict (parent only)."""
    if d is None:
        return
    stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with log_lock:
        if 'master' not in d:
            d['master'] = "[%s][%d] Logging started.\n" % (ts, pid)
        if pid not in d:
            d[pid] = "[%s] Logging started.\n" % ts
        if master:
            d['master'] += "[%s][%d] %s.\n" % (ts, pid, message)
        d[pid] += "[%s] %s.\n" % (ts, message)
        _append_session_log('[%s][%d] %s' % (stamp, pid, message))

def _apply_log_item(item):
    pid, master, ts, message = item
    _write_log(logdict, pid, master, ts, message)

def _flush_log_queue():
    q = log_queue
    if q is None:
        return
    while True:
        try:
            _apply_log_item(q.get_nowait())
        except Empty:
            break
        except (OSError, EOFError, BrokenPipeError):
            break

def _drain_logs():
    while True:
        q = log_queue
        if q is None:
            break
        try:
            item = q.get(timeout=0.2)
        except Empty:
            if log_drain_stop is not None and log_drain_stop.is_set():
                break
            continue
        except (OSError, EOFError, BrokenPipeError):
            break
        _apply_log_item(item)
    _flush_log_queue()

def log( message, logdict, master=False ):
    """
    Add a message to the log for the process (based on PID) calling this function.
    :param message:  The message to add to the log.
    :param logdict: The logging dictionary to log to. Use crunchy.getLogDict().
                    Ignored in worker processes (lines go through log_queue).
    :param master: True if this message should be added to the master log rather than the worker log.
    """
    pid = os.getpid()
    ts = datetime.now().strftime("%H:%M:%S")

    if debug:
        print("[%s][%d] %s.\n"%(ts,pid,message), end='')

    # workers send lines to the parent via the log queue (logdict is None there)
    if log_queue is not None and (logdict is None or (_parent_pid is not None and pid != _parent_pid)):
        log_queue.put((pid, master, ts, message))
        return

    target = logdict if logdict is not None else globals().get('logdict')
    assert target is not None, "[%d] Error - log must not be none. Some multithreading nightmare has occured." % pid
    _write_log(target, pid, master, ts, message)

def error_log_path():
    """
    Today's ``logs/YYYY-MM-DD/errors.txt``.
    Override with settings['error_log'] (a file, or a folder used as log root).
    """
    src = settings
    try:
        if src is not None and src.get('error_log'):
            p = Path(src['error_log']).expanduser()
            if p.suffix:
                return p
            day = p / _log_day()
            day.mkdir(parents=True, exist_ok=True)
            return day / 'errors.txt'
    except Exception:
        pass
    return today_log_dir() / 'errors.txt'

def _format_exception(exc=None):
    """Best-effort exception type, message and traceback for errorlog.txt."""
    import traceback
    import sys
    parts = []
    if exc is not None:
        parts.append('%s: %s' % (type(exc).__name__, exc))
        try:
            parts.append(''.join(traceback.format_exception(exc)).rstrip())
        except Exception:
            tb = getattr(exc, '__traceback__', None)
            try:
                parts.append(''.join(traceback.format_exception(type(exc), exc, tb)).rstrip())
            except Exception:
                pass
    current = traceback.format_exc()
    if current and current.strip() not in ('NoneType: None', 'None'):
        if not parts or current.strip() not in '\n'.join(parts):
            parts.append(current.rstrip())
    elif exc is None:
        info = sys.exc_info()
        if info[1] is not None:
            parts.append('%s: %s' % (type(info[1]).__name__, info[1]))
            try:
                parts.append(''.join(traceback.format_exception(*info)).rstrip())
            except Exception:
                pass
    return '\n'.join(p for p in parts if p)


def log_error(message, exc=None, logdict=None):
    """Write an error to the console / logdict and append it to errorlog.txt."""
    parts = [str(message)]
    detail = _format_exception(exc)
    if detail:
        parts.append(detail)
    text = "\n".join(parts)
    try:
        log(text, logdict if logdict is not None else getLogDict())
    except Exception:
        print(text)
    try:
        path = error_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        block = "======== %s  PID %s ========\n%s\n\n" % (stamp, os.getpid(), text)
        with open(path, 'a', encoding='utf-8') as fh:
            fh.write(block)
    except Exception:
        pass

#################################################################
## Workers: the following setup, run and manage worker processes
#################################################################
def crunch(_queue, _log_queue, _block, _end, _prog, _settings, _ppid, _jobs=None):
    """Read from the priority queue and process or execute corresponding messages"""
    _limit_numeric_threads(1)

    # store globals in this process (spawn children start with a fresh module state)
    global logdict
    global log_queue
    global queue
    global block
    global endwhenempty
    global prog
    global settings
    global _parent_pid
    global worker_jobs

    queue = _queue
    log_queue = _log_queue
    logdict = None
    block = _block
    endwhenempty = _end
    prog = _prog
    settings = _settings
    _parent_pid = _ppid
    worker_jobs = _jobs

    log("Worker initialised", logdict)

    waiting_ram = False
    while True:
        # Finish must be able to drain / exit even if the GUI left us paused
        if bool(block.value) and not bool(endwhenempty.value):
            time.sleep(0.1)
            continue
        # hold off new jobs when RAM is tight; complete() still drains
        if not bool(endwhenempty.value) and not _ram_ok(settings):
            if not waiting_ram:
                avail, _total = _memory_bytes()
                free_gb = (avail / _GB) if avail else 0.0
                log("Low memory (%.1f GB free); waiting to start new jobs" % free_gb, logdict)
                waiting_ram = True
            time.sleep(0.5)
            continue
        if waiting_ram:
            log("Memory recovered; resuming jobs", logdict)
            waiting_ram = False
        try:
            _key, item = _queue.get(True, 0.1)
            msg, value = item
        except Empty:
            if bool(endwhenempty.value):
                break
            continue
        except (OSError, EOFError, BrokenPipeError):
            break
        except Exception as E:
            log_error("Worker failed to read a job from the queue", E, logdict)
            time.sleep(0.1)
            continue
        if msg == "EXEC":
            p = "UNKNOWN"
            try:
                f,args,kwargs = value
                p = args[-3].get('path', "UNKNOWN")
                if _jobs is not None:
                    _jobs[os.getpid()] = str(p)
                prog[p] = 1 # set as in-progress.
                f(*args,**kwargs) # execute function
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as E:
                # keep this process alive; the job is marked complete below
                log_error("Worker job failed for %s" % p, E, logdict)
            finally:
                try:
                    prog[p] = 2
                except Exception:
                    pass
                try:
                    if _jobs is not None:
                        _jobs[os.getpid()] = ''
                except Exception:
                    pass
        else:
            log("Error - %s is an invalid message." % msg, logdict)
    log("Work complete", logdict)

def _spawn_worker():
    """Start one worker process. Caller holds workers_lock if mutating the pool list."""
    p = Process(target=crunch, args=(_raw_queue, log_queue, block, endwhenempty,
                                     prog, settings, _parent_pid, worker_jobs))
    p.daemon = True
    p.start()
    return p

def _note_dead_worker(w):
    """Log a crashed worker and clear any job it left marked as running."""
    pid = getattr(w, 'pid', None)
    path = None
    if worker_jobs is not None and pid is not None:
        try:
            path = worker_jobs.pop(pid, None) or None
        except Exception:
            path = None
    if path:
        try:
            prog[path] = 2
        except Exception:
            pass
        log_error("Worker %s crashed while processing %s; restarting" % (pid, path))
    else:
        log_error("Worker %s exited unexpectedly (code %s); restarting" % (pid, getattr(w, 'exitcode', '?')))

def _watch_workers():
    """Replace workers that die from crashes so the pool stays at the requested size."""
    while True:
        if worker_watch_stop is not None and worker_watch_stop.is_set():
            break
        if workers is None or manager is None:
            break
        with workers_lock:
            for i, w in enumerate(workers):
                if w is None or w.is_alive():
                    continue
                if worker_watch_stop is not None and worker_watch_stop.is_set():
                    break
                # complete() is draining or tearing down — do not bounce workers
                if endwhenempty is not None and bool(endwhenempty.value):
                    continue
                _note_dead_worker(w)
                try:
                    workers[i] = _spawn_worker()
                except Exception as E:
                    log_error("Could not restart worker", E)
        if worker_watch_stop is None:
            time.sleep(1.0)
        elif worker_watch_stop.wait(1.0):
            break

def _stop_aux_threads(timeout=3.0):
    """Ask scout / control threads to exit and join them."""
    global scoutthread
    global crunchthread
    if scout_stop is not None:
        scout_stop.set()
    if control_stop is not None:
        control_stop.set()
    for t in (scoutthread, crunchthread):
        if t is not None and t.is_alive():
            t.join(timeout=timeout)
    scoutthread = None
    crunchthread = None

def _snapshot_and_shutdown_manager():
    """Copy Manager proxies into local dicts, then shut down the Manager server."""
    global manager
    global settings
    global prog
    snap_settings = {}
    snap_prog = {}
    try:
        if settings is not None:
            snap_settings = dict(settings)
    except Exception:
        pass
    try:
        if prog is not None:
            snap_prog = dict(prog)
    except Exception:
        pass
    if manager is not None:
        try:
            manager.shutdown()
        except Exception:
            pass
        manager = None
    settings = snap_settings
    prog = snap_prog
    _release_ipc()


def _release_ipc():
    """Drop queues and shared Values so the resource tracker does not leak semaphores."""
    global log_queue
    global log_drain_stop
    global log_drain_thread
    global queue
    global _raw_queue
    global block
    global endwhenempty
    global worker_jobs
    if log_drain_stop is not None:
        log_drain_stop.set()
    if log_drain_thread is not None and log_drain_thread.is_alive():
        log_drain_thread.join(timeout=2.0)
    log_drain_thread = None
    q = log_queue
    log_queue = None
    if q is not None:
        try:
            q.close()
        except Exception:
            pass
        try:
            q.join_thread()
        except Exception:
            pass
    queue = None
    _raw_queue = None
    block = None
    endwhenempty = None
    worker_jobs = None

def init( nworkers=None ):
    """
    Setup crunchy workers. Must be run from a __main__ = True scope.
    :param nworkers: The number of worker processes for crunchy to use.
    """
    global manager
    global queue
    global _raw_queue
    global block
    global endwhenempty
    global workers
    global logdict
    global log_queue
    global log_drain_stop
    global log_drain_thread
    global file_size_dict
    global known_files
    global new_files
    global file_filter
    global files_lock
    global prog
    global settings
    global _parent_pid
    global worker_jobs
    global worker_watch_stop
    global worker_watch_thread

    apply_config()

    # spawn avoids fork-after-thread deadlocks (scout / log drain live in this process)
    try:
        if get_start_method(allow_none=True) is None:
            set_start_method('spawn')
    except RuntimeError:
        pass

    # pin BLAS before spawn so children inherit the env vars
    _limit_numeric_threads(1)
    _parent_pid = os.getpid()

    # leftover run: stop threads/workers before killing the old Manager
    if worker_watch_stop is not None:
        worker_watch_stop.set()
    _stop_aux_threads()
    if workers:
        with workers_lock:
            pool = list(workers)
        for w in pool:
            if w is not None and w.is_alive():
                try:
                    w.terminate()
                except Exception:
                    pass
        workers = None
    if manager is not None:
        try:
            manager.shutdown()
        except Exception:
            pass
        manager = None
    _release_ipc()

    block = Value('i', 0)
    endwhenempty = Value('i', 0)
    manager = _CrunchyManager()
    manager.start()
    _raw_queue = manager.PriorityQueue()
    queue = PriorityJobQueue(_raw_queue)
    prog = manager.dict()
    settings = manager.dict()
    worker_jobs = manager.dict()

    # logs live in this process; workers send lines over a Queue
    logdict = {}
    log_queue = Queue()

    # scout / control share these in-process (they are threads, not processes)
    files_lock = threading.Lock()
    file_size_dict = {}
    known_files = {}
    new_files = {}
    file_filter = {}

    # populate default settings
    for k,v in workflow_settings.items():
        settings[k] = v['value']
    for k,v in crunchy_settings.items():
        settings[k] = v['value']

    # start workers before the log-drain thread so a leftover fork start method
    # does not copy an extra thread into the child
    workers = []
    if nworkers is None:
        nworkers = settings['nthreads']
    requested = int(nworkers)
    nworkers = _cap_workers_for_ram(requested, settings)
    if nworkers < requested:
        log("Low memory; starting %d worker(s) instead of %d" % (nworkers, requested),
            logdict, master=True)
    for i in range(int(nworkers)):
        workers.append(_spawn_worker())

    worker_watch_stop = threading.Event()
    worker_watch_thread = threading.Thread(target=_watch_workers, name='crunchy-watch', daemon=True)
    worker_watch_thread.start()

    if log_drain_thread is None or not log_drain_thread.is_alive():
        log_drain_stop = threading.Event()
        log_drain_thread = threading.Thread(target=_drain_logs, name='crunchy-logs', daemon=True)
        log_drain_thread.start()

def initialised():
    """
    Return True if crunchy has been inititalised and worker processes have spawned (and are awaiting or executing tasks).
    """
    return workers is not None

def running():
    """
    Return True if crunchy has been inititalised and a scout thread is running to pass jobs to worker processes.
    """
    return initialised() and scoutthread is not None and scoutthread.is_alive()

def pause():
    """
    Pause worker processes. Currently running jobs will be finished, but new jobs will not be launched
    from the queue. This will not pause file scouts.
    """
    global block
    if block is not None:
        block.value = 1

def paused():
    global block
    return block is not None and block.value == 1

def resume():
    """
    Resume paused worker processes so that they continue taking new jobs from the queue.
    """
    global block
    if block is not None:
        block.value = 0

def wait():
    """
    Block until the queue is empty and no job is marked as running.
    """
    while True:
        q_empty = queue is None or queue.empty()
        running_jobs = False
        if prog is not None:
            try:
                running_jobs = any(v == 1 for v in list(prog.values()))
            except Exception:
                running_jobs = False
        if q_empty and not running_jobs:
            return
        time.sleep(0.5)

def complete(join = True, end=True):
    """
    Tell all workers to finish once the queue is empty.

    :param join: True if this thread should block until all workers have completed. Default is True.
    :param end: True if all processes/threads (including the file scout) should be shut down after work is finished. Default is True.
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
    global worker_watch_stop
    global worker_watch_thread
    if endwhenempty is None:
        return

    # paused workers would otherwise never see endwhenempty
    resume()
    endwhenempty.value = 1
    # stop the respawner first so Terminate cannot bounce a new process
    if end and worker_watch_stop is not None:
        worker_watch_stop.set()
        if worker_watch_thread is not None and worker_watch_thread.is_alive():
            worker_watch_thread.join(timeout=2.0)
    if join and workers is not None:
        # join current workers; a crash during drain may respawn one more
        for _ in range(8):
            with workers_lock:
                current = [w for w in workers if w is not None]
            if not current or all(not w.is_alive() for w in current):
                break
            for w in current:
                w.join()
    _flush_log_queue()
    if end:
        if workers is not None:
            with workers_lock:
                pool = list(workers)
            for w in pool:
                if w is not None and w.is_alive():
                    w.terminate()
        workers = None
        _stop_aux_threads()
        scoutdirs.clear()

    # and finally, run any "finish" jobs ( in this thread )
    if settings is not None:
        for k, v in finalize.items():
            log('Running final tasks: [%s]' % k, logdict, master=True )
            v(Path(settings['inpath']), Path(settings['outpath']), settings)
    _flush_log_queue()

    if end:
        _snapshot_and_shutdown_manager()

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

def watch( path, depth=0, restart=True ):
    """
    Add a hot directory and, if a scout is already running, restart it so the
    new path is picked up without forgetting already-known files.

    :param path: directory to search
    :param depth: scout depth (0 = list this directory)
    :param restart: restart a live scout (default True)
    :return: the path as stored in scoutdirs
    """
    path = str(Path(path))
    add(path, int(depth), clear=False)
    if restart and scoutthread is not None and settings is not None:
        scout(clear=False)
    return path

def scout( clear = True ):
    """
    Start crunchy scout, which will begin passing files to worker processes. If a scout has already been launched,
    then it will be stopped and a new one run (in case e.g. settings have changed). Note that this will not
    launch a thread passing identified files to the workflow - use crunchy.run() to do this.

    :param clear: True if the known_files, file_size_dict and new_files dictionaries should be emptied before launching
               or re-launching this scout. Default is True.
    """
    global scoutthread
    global scout_stop

    assert len(scoutdirs) > 0, "Error - cannot start crunchy without any hot directories."

    # stop old scout if it is running (threads cannot be terminate()'d)
    if scoutthread is not None:
        if scout_stop is not None:
            scout_stop.set()
        if scoutthread.is_alive():
            scoutthread.join(timeout=3.0)
        scoutthread = None

    # clear file dictionaries
    if clear:
        ctx = files_lock if files_lock is not None else contextlib.nullcontext()
        with ctx:
            new_files.clear()
            known_files.clear()
            file_size_dict.clear()
            if file_filter is not None:
                file_filter.clear()

    # launch new scout thread (I/O bound; no need for a process)
    from .base import scout as _scout_fn
    scout_stop = threading.Event()
    scoutthread = threading.Thread(
        target=_scout_fn,
        args=(list(scoutdirs.keys()),
              list(scoutdirs.values()),
              new_files,
              known_files,
              file_size_dict,
              int(settings.get('wait',1)),
              float(settings.get('idle',1.0))),
        kwargs=dict(stop=scout_stop, lock=files_lock),
        daemon=True,
        name='crunchy-scout',
    )
    scoutthread.start()
    return scoutthread

def setOutpath( path ):
    settings['outpath'] = Path(path)
    crunchy_settings['outpath']['value'] = Path(path)

def setInpath( path ):
    settings['inpath'] = Path(path)
    crunchy_settings['inpath']['value'] = Path(path)

_FILTER_LABEL = {
    -1: 'error',
    0: 'ignored',
    1: 'waiting',
    2: 'accepted',
}
_PROG_LABEL = {0: 'queued', 1: 'running', 2: 'done'}
_RECHECK_STATES = frozenset(('ignored', 'error', 'known', 'done', 'accepted'))


def _bytes_label(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return ''
    if n < 1024:
        return '%d B' % int(n)
    for unit, step in (('KB', 1024.0), ('MB', 1024.0 ** 2), ('GB', 1024.0 ** 3)):
        if n < step * 1024 or unit == 'GB':
            return '%.1f %s' % (n / step, unit)
    return '%d B' % int(n)


def file_overview():
    """
    Snapshot of every path the scout or filters have seen.

    Each row is a dict: path, state, size, size_label, visits, filters, can_recheck.
    """
    from crunchy.base import trigger
    ctx = files_lock if files_lock is not None else contextlib.nullcontext()
    with ctx:
        sizes = dict(file_size_dict or {})
        news = dict(new_files or {})
        known = dict(known_files or {})
        decisions = dict(file_filter or {})
    prog_map = {}
    if prog is not None:
        try:
            prog_map = dict(prog)
        except Exception:
            prog_map = {}

    rows = {}

    def _row(path, **kw):
        key = str(path)
        r = rows.setdefault(key, dict(
            path=key, state='known', size=None, visits=None,
            filters='', can_recheck=False,
        ))
        for k, v in kw.items():
            if v is not None:
                r[k] = v
        return r

    for p, val in sizes.items():
        if isinstance(val, (tuple, list)) and val:
            _row(p, state='watching', size=val[0], visits=val[1] if len(val) > 1 else None)
        else:
            _row(p, state='watching', size=val)
    for p, s in news.items():
        _row(p, state='new', size=s)
    for p, s in known.items():
        _row(p, state='known', size=s, can_recheck=True)
    for p, decs in decisions.items():
        if not decs:
            continue
        if isinstance(decs, tuple) and len(decs) == 2 and not isinstance(decs[0], (list, tuple)):
            decs = [decs]
        best = None
        parts = []
        for item in decs:
            try:
                name, status = item
            except (TypeError, ValueError):
                continue
            parts.append('%s → %s' % (name, _FILTER_LABEL.get(status, status)))
            if best is None or status > best:
                best = status
        label = _FILTER_LABEL.get(best, 'known') if best is not None else 'known'
        _row(p, state=label, filters=', '.join(parts),
             can_recheck=best in (trigger.REJECT, trigger.ERROR))
    for p, st in prog_map.items():
        _row(p, state=_PROG_LABEL.get(st, str(st)), can_recheck=(st == 2))

    out = []
    counts = {}
    for r in rows.values():
        r['size_label'] = _bytes_label(r['size']) if r['size'] is not None else ''
        r['can_recheck'] = bool(r['can_recheck'] and r['state'] in _RECHECK_STATES)
        counts[r['state']] = counts.get(r['state'], 0) + 1
        out.append(r)
    order = {s: i for i, s in enumerate(
        ('running', 'queued', 'new', 'watching', 'waiting', 'ignored', 'error',
         'accepted', 'done', 'known'))}
    out.sort(key=lambda r: (order.get(r['state'], 99), r['path']))
    return dict(rows=out, counts=counts)


def recheck(paths):
    """
    Forget scout/filter memory for ``paths`` and put them back on the
    next filter pass (as if they were newly stable).
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]
    paths = [str(p) for p in paths if p]
    if not paths:
        return 0
    from crunchy.base.scout import _getFileSize
    ctx = files_lock if files_lock is not None else contextlib.nullcontext()
    with ctx:
        if known_files is None or new_files is None:
            return 0
        for p in paths:
            known_files.pop(p, None)
            if file_filter is not None:
                file_filter.pop(p, None)
            if file_size_dict is not None:
                file_size_dict.pop(p, None)
            try:
                size = _getFileSize(p)
            except Exception:
                size = 0
            new_files[p] = size
    if prog is not None:
        for p in paths:
            try:
                prog.pop(p, None)
            except Exception:
                pass
    if logdict is not None:
        log('Recheck %d path(s)' % len(paths), logdict, master=True)
    return len(paths)


def process_new_files( settings, new_files, known_files, entries, outpath, _logdict ):
    """
    Process any new files in the new_files dictionary and pass them to each listening filefilter to trigger
    workflow events.
    """
    assert settings.get('outpath',None) is not None, 'Error - no outpath set. See setOutpath()'
    os.makedirs(settings['outpath'], exist_ok=True)

    from crunchy.base import trigger # here to circular imports
    if new_files is None:
        assert False, 'Error - a serious multithreading failure has occurred...'

    # snapshot then clear so the scout can keep writing
    ctx = files_lock if files_lock is not None else contextlib.nullcontext()
    with ctx:
        batch = list(new_files.items())
        new_files.clear()
        for p, s in batch:
            known_files[p] = s

    waiting = []
    for p, s in batch:
        decisions = []
        for name, f in entries.items():
            status = f(Path(p), Path(settings['outpath']), settings )
            decisions.append((name, status))
            if status == trigger.WAIT:
                waiting.append(p)
        if file_filter is not None:
            with ctx:
                file_filter[str(p)] = decisions
    if waiting:
        with ctx:
            for p in waiting:
                known_files.pop(p, None)

def _serve():
    """
    Control loop: pass newly scouted files to file filters (runs in a thread).
    """
    log('Spawning control thread.', logdict)
    while control_stop is not None and not control_stop.is_set():
        try:
            if settings is None or manager is None:
                break
            outpath = settings.get('outpath')
            process_new_files(settings, new_files, known_files, entries, outpath, logdict)
        except (OSError, EOFError, BrokenPipeError):
            break
        except Exception as E:
            log_error("Control thread failed while filtering new files", E)
        if control_stop is None or control_stop.wait(1.0):
            break

def run():
    """
    Run crunchy in a separate thread that loops continuously looking for new files and triggering events on the
    worker queue. Note that this will automatically launch a scout thread also by calling crunchy.scout().
    :return: The thread crunchy is running in. This can be used to e.g., join() the main thread.
    """
    global crunchthread
    global control_stop

    apply_config()

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
        result = v( Path(settings['inpath']), Path(settings['outpath']), settings )
        if result is False:
            succ = False
            break
    if not succ:
        print("Crunchy workflow could not start due to errors during initialisation.")
        return None

    # start scout thread
    scout()

    # launch control thread (path filters are cheap; keep them in-process)
    if crunchthread is not None and crunchthread.is_alive():
        if control_stop is not None:
            control_stop.set()
        crunchthread.join(timeout=3.0)
    control_stop = threading.Event()
    crunchthread = threading.Thread(target=_serve, name='crunchy-control', daemon=True)
    crunchthread.start()
    return crunchthread


# apply packaged / local crunchy.ini defaults once at import
apply_config()
