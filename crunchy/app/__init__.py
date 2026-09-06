from flask import Flask , render_template, render_template_string, send_file, abort, send_from_directory, request, make_response
from pathlib import Path
from datetime import datetime
from natsort import natsorted
import crunchy
import json
import re
import socket
import signal
import threading
import time
import webbrowser
from glob import glob

# set numpy to run in a single thread
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'

# globals
root = Path('/')
flaskapp = None
server = None

_PREFS_COOKIE = 'crunchy-prefs'
_PREFS_MAX_AGE = 60 * 60 * 24 * 365  # one year


def _workflow_pref_id():
    """Stable id for the loaded workflow (its setting keys, not shared paths)."""
    keys = ','.join(sorted(crunchy.workflow_settings.keys()))
    return keys or 'default'


def _current_pref_slot():
    slot = {'crunchy': {}, 'workflow': {}}
    for label, schema in (('crunchy', crunchy.crunchy_settings),
                          ('workflow', crunchy.workflow_settings)):
        for key, entry in schema.items():
            if not isinstance(entry, dict) or 'value' not in entry:
                continue
            val = entry['value']
            slot[label][key] = str(val) if isinstance(val, Path) else val
    return slot


def _prefs_payload(existing=None):
    """Cookie JSON: one slot per workflow so dummy/sensor prefs do not clobber each other."""
    slots = {}
    if isinstance(existing, dict):
        if isinstance(existing.get('by_workflow'), dict):
            slots.update(existing['by_workflow'])
        elif 'crunchy' in existing or 'workflow' in existing:
            slots[_workflow_pref_id()] = {
                'crunchy': existing.get('crunchy') or {},
                'workflow': existing.get('workflow') or {},
            }
    wid = _workflow_pref_id()
    slots[wid] = _current_pref_slot()
    return {'id': wid, 'by_workflow': slots}


def _slot_for_current(data):
    if not isinstance(data, dict):
        return None
    slots = data.get('by_workflow')
    if isinstance(slots, dict):
        return slots.get(_workflow_pref_id())
    wkeys = set((data.get('workflow') or {}).keys())
    current = set(crunchy.workflow_settings.keys())
    if wkeys and wkeys.isdisjoint(current):
        return None
    return {'crunchy': data.get('crunchy') or {}, 'workflow': data.get('workflow') or {}}


def _apply_prefs(data):
    slot = _slot_for_current(data)
    if not slot:
        return
    crunchy._apply_to_schema(crunchy.crunchy_settings, slot.get('crunchy') or {})
    crunchy._apply_to_schema(crunchy.workflow_settings, slot.get('workflow') or {})


def _load_pref_cookie():
    raw = request.cookies.get(_PREFS_COOKIE)
    if not raw:
        return
    try:
        _apply_prefs(json.loads(raw))
    except Exception:
        pass


def _stop_flask():
    """Ask Flask's main thread to return so atexit can free semaphores."""
    def _die():
        time.sleep(0.4)
        try:
            os.kill(os.getpid(), signal.SIGINT)
        except Exception:
            os._exit(0)
    threading.Thread(target=_die, daemon=True).start()


def _shutdown_app():
    """Stop workers (if any) and schedule the HTTP server to exit."""
    if crunchy.initialised():
        try:
            crunchy.complete(False, True)
        except Exception:
            pass
    _stop_flask()


def _with_pref_cookie(html, save=False):
    resp = make_response(html)
    if save:
        existing = None
        raw = request.cookies.get(_PREFS_COOKIE)
        if raw:
            try:
                existing = json.loads(raw)
            except Exception:
                existing = None
        try:
            resp.set_cookie(
                _PREFS_COOKIE,
                json.dumps(_prefs_payload(existing), default=str),
                max_age=_PREFS_MAX_AGE,
                samesite='Lax',
                path='/',
            )
        except Exception:
            pass
    return resp


def _ui_info():
    """Cheap snapshot for the header and status pills."""
    workers = crunchy.workers or []
    alive = sum(1 for w in workers if w is not None and w.is_alive())
    info = dict(
        running=bool(crunchy.running()),
        paused=bool(crunchy.paused()) if crunchy.block is not None else False,
        nworkers=len(workers),
        alive=alive,
        ram_free=None,
        ram_total=None,
        ram_low=False,
        disk_free=None,
        disk_total=None,
        disk_low=False,
        error_path=None,
        error_exists=False,
        error_mtime=None,
        inpath=None,
        outpath=None,
        wait=None,
        idle=None,
    )
    try:
        avail, total = crunchy._memory_bytes()
        if avail is not None and total:
            info['ram_free'] = round(avail / crunchy._GB, 1)
            info['ram_total'] = round(total / crunchy._GB, 1)
    except Exception:
        pass
    src = crunchy.settings if crunchy.settings is not None else None
    def _get(key, fallback=None):
        if src is not None:
            try:
                if key in src:
                    return src[key]
            except Exception:
                pass
        entry = crunchy.crunchy_settings.get(key)
        return entry.get('value') if entry else fallback
    try:
        reserve = float(_get('ram_reserve', 4) or 0)
        info['ram_low'] = info['ram_free'] is not None and info['ram_free'] < reserve
        info['wait'] = _get('wait')
        info['idle'] = _get('idle')
        info['inpath'] = _get('inpath')
        info['outpath'] = _get('outpath')
        if info['outpath']:
            free, total = crunchy._disk_bytes(info['outpath'])
            if free is not None and total:
                info['disk_free'] = round(free / crunchy._GB, 1)
                info['disk_total'] = round(total / crunchy._GB, 1)
                info['disk_low'] = info['disk_free'] < crunchy._DISK_WARN_GB
    except Exception:
        pass
    try:
        path = crunchy.error_log_path()
        info['error_path'] = str(path)
        if path.is_file() and path.stat().st_size > 0:
            info['error_exists'] = True
            info['error_mtime'] = datetime.fromtimestamp(path.stat().st_mtime).strftime('%H:%M:%S')
    except Exception:
        pass
    return info


def _watch_from_form(req_path):
    """Register a directory from the Directories tab as a scout path."""
    raw = crunchy._strip_wrapping_quotes(request.form.get('watch_path') or req_path or '')
    if not raw:
        return 'Enter a folder path.'
    try:
        depth = int(request.form.get('watch_depth', 0) or 0)
    except (TypeError, ValueError):
        depth = 0
    depth = max(0, min(depth, 50))
    root_resolved = root.resolve()
    candidate = Path(raw).expanduser()
    if candidate.is_absolute():
        path = candidate.resolve()
    else:
        path = (root / candidate).resolve()
        try:
            path.relative_to(root_resolved)
        except ValueError:
            return 'Relative path is outside the working directory.'
    if not path.is_dir():
        return 'Not a directory: %s' % path
    crunchy.watch(path, depth=depth, restart=True)
    if crunchy.getLogDict() is not None:
        crunchy.log('Watching %s at depth %d' % (path, depth), crunchy.getLogDict(), True)
    if crunchy.running():
        return 'Scout is now searching %s (depth %d).' % (path, depth)
    return 'Added %s (depth %d). It will be searched after Launch.' % (path, depth)


def _list_dir(req_path, notice=None):
    root_resolved = root.resolve()
    path = (root / req_path).resolve()
    try:
        path.relative_to(root_resolved)
    except ValueError:
        abort(404)
    if not os.path.exists(path):
        abort(404)
    if os.path.isfile(path):
        return send_file(path)
    content = natsorted(os.listdir(path))
    files = [f for f in content if os.path.isfile(path / f)]
    dirs = [f for f in content if os.path.isdir(path / f)]
    if path != root_resolved:
        pdir = str(path.parent.relative_to(root_resolved))
        if pdir == '.':
            pdir = ''
    else:
        pdir = ''
    return render_template('directories.html',
                           parent_dir=pdir,
                           current=req_path,
                           abs_current=str(path),
                           dirs=dirs,
                           files=files,
                           basename=os.path.basename,
                           notice=notice,
                           watching=crunchy.scoutdirs.get(str(path)),
                           scoutdirs=dict(crunchy.scoutdirs))

def _pick_port(preferred=5001):
    """Return preferred port if free, otherwise an ephemeral free port."""
    for port in (preferred, 0):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('127.0.0.1', port))
            chosen = sock.getsockname()[1]
            return chosen
        except OSError:
            continue
        finally:
            try:
                sock.close()
            except OSError:
                pass
    return preferred


def run( basepath, open_browser=False, port=5001 ):
    """
    Launch the crunchy app.

    :param basepath: working directory for relative paths and the file browser
    :param open_browser: if True, open the UI in the default browser
    :param port: preferred port (falls back to a free port if busy)
    """

    crunchy.apply_config()

    # store basepath for files and crunchy
    global root
    root = Path(basepath)

    # setup app
    global flaskapp
    flaskapp = Flask(__name__)
    flaskapp.config['TEMPLATES_AUTO_RELOAD'] = True

    @flaskapp.context_processor
    def _inject_ui():
        return dict(ui=_ui_info(), crunchy=crunchy)

    @flaskapp.route('/', methods=['GET','POST'])
    def index():
        errors = []
        checked = [] # checkboxes that have been set - all others should be changed to False!
        launch = False # becomes true if we want (and can) launch crunchy!
        save_prefs = False
        if request.method == 'GET' and not crunchy.running():
            _load_pref_cookie()
        if request.method == 'POST': # handle POST requests
            for k,v in request.form.items():
                # print(k,v)
                try:
                    target, key = k.split('__')
                except:
                    print("Warning: could not parse input of name %s" % k)
                    continue

                if 'action' in target: # this was an action button
                    if 'launch' in key.lower():
                        launch = True
                    elif 'pause' in key.lower():
                        crunchy.pause()
                        crunchy.log('Crunchy is taking a break', crunchy.getLogDict(), True)
                    elif 'resume' in key.lower():
                        crunchy.resume()
                        crunchy.log('Resuming work', crunchy.getLogDict(), True)
                    elif 'finish' in key.lower():
                        crunchy.log('Finishing jobs... please wait', crunchy.getLogDict(), True)
                        crunchy.resume() # paused workers otherwise never drain
                        crunchy.complete(True,True)
                        crunchy.log('Workflow complete', crunchy.getLogDict(), True)

                    elif 'terminate' in key.lower():
                        crunchy.complete(False, True)
                        crunchy.log('Workers have been brutally terminated.', crunchy.getLogDict(), True)
                    elif 'shutdown' in key.lower():
                        _shutdown_app()
                        page = render_template('shutdown.html')
                        return _with_pref_cookie(page, save=False)
                else: # we are updating settings
                    if 'crunchy' in target: # update crunchy settings
                        target = crunchy.crunchy_settings
                    elif 'workflow' in target: # update  workflow settings
                        target = crunchy.workflow_settings

                    # convert to correct type (and validate that this is possible)
                    dtype = target[key].get('type','string').lower()
                    try:
                        if dtype == 'string' or dtype=='select':
                            v = crunchy._strip_wrapping_quotes(v) if dtype == 'string' else str(v)
                        elif dtype == 'path':
                            v = Path(crunchy._strip_wrapping_quotes(v)).expanduser()
                            if target[key].get('mustexist', False):
                                assert os.path.exists(root / v)
                        elif dtype == 'float':
                            v = float(v)
                        elif dtype == 'int':
                            v = int(v)
                        elif dtype == 'bool':
                            checked.append(key)
                            v = True # only "on" checkboxes are passed to the POST request
                    except:
                        errors.append(key)
                        continue
                    target[key]['value'] = v
                    save_prefs = True

            # If no errors, launch!
            if len(errors) == 0 and launch:
                # update paths to be absolute and booleans to be false unless set in the POST request
                for d in [crunchy.crunchy_settings, crunchy.workflow_settings]:
                    for k,v in d.items():
                        if v['type'] == 'path':
                            p = Path(crunchy._strip_wrapping_quotes(v['value'])).expanduser()
                            v['value'] = p if p.is_absolute() else (root / p)
                        if v['type'] == 'bool':
                            if k not in checked:
                                v['value'] = False

                # spawn worker threads
                crunchy.init()
                crunchy.log('Starting workflow', crunchy.getLogDict(), True)
                thread = crunchy.run() # run workers
                if thread == None: # this failed
                    crunchy.complete(False, True)
                    launch = False

        # is crunchy running
        if crunchy.running(): # render status
            page = render_template('index.html', log=crunchy.getLogDict(), crunchy=crunchy,
                                   reversed=reversed, len=len, enumerate=enumerate)
        else: # render settings / setup
            page = render_template('settings.html', cset=crunchy.crunchy_settings, root=str(root),
                                   wset=crunchy.workflow_settings, errors=errors,
                                   enumerate=enumerate, )
        return _with_pref_cookie(page, save=save_prefs)
    
    @flaskapp.route('/status', methods=['GET', 'POST'])
    def workflow():
        return render_template('status.html', settings=crunchy.settings, root=str(root), enumerate=enumerate, )
    
    @flaskapp.route('/debug', methods=['GET', 'POST'])
    def debug():
        return abort(404)

    @flaskapp.route('/errors')
    def errors():
        path = crunchy.error_log_path()
        text = ''
        exists = False
        try:
            if path.is_file() and path.stat().st_size > 0:
                exists = True
                data = path.read_bytes()
                if len(data) > 32768:
                    data = data[-32768:]
                text = data.decode('utf-8', errors='replace')
        except Exception as E:
            text = 'Could not read error log: %s' % E
        return render_template('errors.html', path=str(path), text=text, exists=exists)

    @flaskapp.route('/directories', defaults={'req_path': ''}, methods=['GET', 'POST'])
    @flaskapp.route('/directories/<path:req_path>', methods=['GET', 'POST'])
    def browse_directories(req_path):
        notice = None
        if request.method == 'POST' and 'action__watch' in request.form:
            notice = _watch_from_form(req_path)
        return _list_dir(req_path, notice=notice)

    @flaskapp.route('/files', methods=['GET', 'POST'])
    def scout_files():
        notice = None
        if request.method == 'POST':
            paths = []
            all_ignored = 'action__recheck_all_ignored' in request.form
            if all_ignored:
                overview = crunchy.file_overview()
                paths = [r['path'] for r in overview['rows'] if r['state'] == 'ignored']
            elif 'action__recheck_selected' in request.form:
                paths = request.form.getlist('path')
            elif request.form.get('action__recheck'):
                paths = [request.form.get('action__recheck')]
            if paths:
                n = crunchy.recheck(paths)
                notice = 'Queued %d path(s) for another filter pass.' % n
            elif all_ignored:
                notice = 'No ignored files to recheck.'
            else:
                notice = 'Select one or more ignored files to recheck.'
        overview = crunchy.file_overview()
        return render_template('files.html', notice=notice,
                               rows=overview['rows'], counts=overview['counts'])

    @flaskapp.route('/dashboard', methods=['GET', 'POST'])
    def dashboard():
        if request.method == 'POST': # handle POST requests
            pass
        else:
            if crunchy.running() and crunchy.initialised():

                return render_template_string( crunchy.dashboard, root=str(root),
                                               crunchy=crunchy, inpath=str(crunchy.settings['inpath']),
                                               outpath=str(crunchy.settings['outpath']),
                                               reversed=reversed, len=len, re=re, glob=glob, os=os, natsorted=natsorted)
            else:
                return "Crunchy is not running. Please start it before viewing dashboard."

    @flaskapp.route('/<path:req_path>')
    def dir_listing(req_path):
        # dashboard / relative image links still use paths under basepath
        if req_path in ('errors', 'files', 'directories', 'status', 'dashboard', 'debug'):
            return abort(404)
        return _list_dir(req_path)

    # run
    port = _pick_port(port)
    url = 'http://127.0.0.1:%d' % port
    print('Crunchy UI: %s' % url)
    if open_browser:
        def _open():
            time.sleep(0.6)
            webbrowser.open(url)
        threading.Thread(target=_open, daemon=True).start()
    flaskapp.run(threaded=False, processes=1, host="0.0.0.0", port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    import sys,os

    # parse arguments [ workflow basepath ]
    n_cpu = os.cpu_count() or 2
    nworkers = max(1, n_cpu - 1)
    path = os.getcwd()
    flow = None
    try:
        if len(sys.argv) < 2:
            print("Please specify workflow. Options are: dummy")
            sys.exit(1)
        flow = sys.argv[1] # workflow to run
        if len(sys.argv) >= 3:
            path = sys.argv[2] # path to run in
        if len(sys.argv) >= 4:
            nworkers = int(sys.argv[3]) # number of worker processes to use
    except SystemExit:
        raise
    except:
        print("Error - incorrect arguments. Should be: workflow_name [base_path] [nthreads].")
        sys.exit(1)

    print("Launching crunchy workflow %s in %s" % (flow,path))
    import crunchy
    __import__("crunchy.workflows.%s"%flow) # import workflow
    print("Spawning %d worker processes."%nworkers)
    crunchy.init(nworkers)
    run( path )