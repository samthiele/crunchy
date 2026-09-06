# crunchy API Reference

GitHub: https://github.com/samthiele/crunchy — docs: https://samthiele.github.io/crunchy/crunchy.html

Version in `setup.py`: **0.25**. License: GPL-2.0.

```python
import crunchy
from crunchy.base.trigger import init, finish, fileTrigger
import crunchy.base.trigger as trigger
from crunchy.base.errors import logAndStop, logAndContinue
from crunchy.base.mirror import mirror
from crunchy.app import run
```

## Lifecycle

| Function | Role |
|----------|------|
| `init(nworkers=None)` | Manager (`settings`, `prog`, priority queue); local scout dicts; spawn worker **processes** (`nthreads` if omitted). Pins BLAS/OMP threads to 1 |
| `initialised()` | Workers exist |
| `running()` | Workers + live scout thread |
| `add(path, depth, clear=False)` | Register a hot directory |
| `watch(path, depth=0, restart=True)` | `add` then restart a live scout without clearing known files |
| `scout(clear=True)` | Start / restart the scout **thread** |
| `run()` | `@init` setup (blocking, main thread; `None`/`True` succeed, only `False` cancels) → `scout()` → control **thread**. Returns `None` if setup fails |
| `setInpath(path)` / `setOutpath(path)` | Update `settings` and `crunchy_settings` |
| `pause()` / `resume()` / `paused()` | Stop / start taking jobs from the queue (scout keeps running) |
| `wait()` | Sleep until the queue is empty and no job is running |
| `complete(join=True, end=True)` | Resume if paused; `endwhenempty`; join or terminate workers; stop scout/control threads; run `@finish`; shut down Manager |
| `getProgress(status=0)` | Paths with status 0 queued, 1 running, 2 done |
| `file_overview()` | Scout/filter/queue snapshot (`rows`, `counts`) for the Files tab |
| `recheck(paths)` | Drop ignore/known memory and put paths back on the next filter pass |
| `getQueue()` / `getLogDict()` / `log(msg, logdict, master=False)` / `printLog()` | Queue and per-PID logs |
| `log_error(msg, exc=None, logdict=None)` / `error_log_path()` / `session_log_path()` / `today_log_dir()` | Daily `logs/YYYY-MM-DD/errors.txt` and `log.txt` |
| `apply_config(path=None, force=False)` | Load `crunchy.ini` / `crunchy.json` defaults |
| `read_ini(path, *schemas, missing_ok=False)` | Parse a workflow `.ini` / `.json`; optionally apply matching keys to setting schemas |

`init()` docstring: call from a `__main__` scope.

## Trigger module (`crunchy.base.trigger`)

```python
@init
def setup(indir, outdir, settings): ...

@finish
def final_tasks(indir, outdir, settings): ...

@fileTrigger(flow=[...], fail=logAndStop, block=False, vb=1, priority=0)
def filter(data, outpath, settings):
    return trigger.PROCESS, outpath  # or REJECT / WAIT / ERROR
    # optional per-file override: return trigger.PROCESS, outpath, 10
```

| Flag | Effect |
|------|--------|
| `flow` | Functions run in order on success |
| `fail` | `fail(logdict, exc, function=, data=, outpath=, settings=)` — `False` aborts the remaining flow |
| `block=True` | Run `_job` in the control thread (no queue) |
| `vb` | 0–3 log verbosity |
| `priority` | Integer; **higher runs first**. Equal priorities stay FIFO. Override per file with a 3-tuple return |

`logAndStop` → `False`; `logAndContinue` → `True`.

Registries (populated on import): `crunchy.setup`, `crunchy.entries`, `crunchy.finalize`.

## Global settings

`crunchy.crunchy_settings` (GUI “Crunchy inputs”):

| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `inpath` | path | `CrunchyIn` | `mustexist=True` |
| `outpath` | path | `CrunchyOut` | created if needed |
| `nthreads` | int | 2 | `min=1`, `max=max(1, cpu_count-1)` |
| `wait` | int | 5 | stable-size visit count |
| `idle` | float | 1.0 | scout sleep (seconds) |
| `ram_reserve` | float | 4.0 | Pause new jobs if free RAM is below this many GB. `0` disables. `init()` also starts a single worker if already below the threshold |

`crunchy.workflow_settings` — workflow-defined; same schema: `type`, `value`, optional `min`/`max`/`options`/`desc`/`mustexist`.

Defaults also live in `crunchy/crunchy.ini` (shipped in the wheel via `package_data` / `MANIFEST.in`). Search order, later wins: package `crunchy.ini` → clone-root `crunchy.ini` / `crunchy.json` → cwd → `$CRUNCHY_CONFIG`. `[crunchy]` keys set `crunchy_settings`. Any key that matches a workflow setting overrides that workflow default. JSON may be flat or use `"crunchy"` / `"workflow"` objects. `apply_config()` runs at import (crunchy keys) and again on `init()` / `run()` / Flask `run()` (workflow keys). Scripts may still overwrite values afterwards.

After `init()`, resolved values live in the shared `crunchy.settings` dict.

`crunchy.debug = True` prints logs to the console (not shared across processes).

## Scout and mirror

```python
from crunchy.base.scout import scout
scout(paths, depth, new_files, known_files, file_size_dict,
      wait=5, idle=5.0, maxiter=None, stop=None, lock=None)
```

Directory size is the sum of contained files (symlinks skipped).

```python
from crunchy.base.mirror import mirror
mirror(local_path, remote_path, sleeptime=5.0, maxiter=np.inf,
       delete=False, debug=True)
```

## Flask app (`from crunchy.app import run`)

`run(basepath, open_browser=False, port=5001)` — `host=0.0.0.0`, `threaded=False`, `processes=1`, `use_reloader=False`. Busy port falls back to a free one. `open_browser=True` opens `http://127.0.0.1:<port>` via stdlib `webbrowser`.

| Route | Role |
|-------|------|
| `/` GET/POST | Launchpad or live status (pause / resume / finish / terminate / shutdown; settings POST). Last-used settings stored in the `crunchy-prefs` cookie (per workflow). Shutdown stops workers and exits Flask |
| `/status` | Live settings + RAM / worker pills |
| `/errors` | Tail of today's `logs/YYYY-MM-DD/errors.txt` |
| `/directories` / `/directories/<path>` | Folder browser under `basepath`. POST `action__watch` adds the folder to the scout (`watch_depth`, default 0) |
| `/files` | Scout inventory (watching / queued / ignored). POST `action__recheck` / `action__recheck_selected` / `action__recheck_all_ignored` |
| `/dashboard` | `render_template_string(crunchy.dashboard, …)` if running |
| `/debug` | 404 — disabled |
| `/<path>` | File browser / `send_file` under `basepath` (dashboard image links) |

`crunchy/app/__init__.py` also has a `__main__` CLI: `workflow_name [base_path] [nthreads]` (`__import__("crunchy.workflows.%s" % flow)`). Only **`dummy`** is shipped.

## Layout

```
crunchy/
  __init__.py          # settings, workers, scout, run, complete
  base/                # trigger, scout, mirror, errors
  workflows/dummy.py   # example
  app/                 # Flask (run lives in __init__.py)
    app.py             # local launcher with hard-coded paths
launch.py              # dummy + run(Path.home())
tests/
demonstration.ipynb
```

## Tests

| File | Covers |
|------|--------|
| `test_base.py` | `_scrape_`, `mirror`, `scout` visit/size, `init`/`pause`/`complete`, pause+finish, priority queue, RAM guard, worker survives error, worker respawn. `test_workers` clears `finalize` so dummy’s `@finish` (imported at collection) does not add an extra log PID |
| `test_dummy.py` | isolated dummy steps, `fileTrigger` queue, full dummy run (60 s) |

Not tested: Flask routes, dashboard, `mirror(delete=True)`.
