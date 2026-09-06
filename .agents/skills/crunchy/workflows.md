# crunchy workflows

Snippets from `demonstration.ipynb`, `crunchy/workflows/dummy.py`, and `tests/`.

## 1. Write a workflow

Put this in `crunchy/workflows/<name>.py` (or a notebook; do not re-run decorator cells).

```python
import crunchy
from crunchy.base.trigger import fileTrigger, init, finish
import crunchy.base.trigger as trigger

crunchy.workflow_settings = dict(
    name=dict(type='string', value='bigdata', desc='free text'),
    set_count=dict(type='int', value=8, min=3, max=8, desc=''),
    assemble=dict(type='bool', value=True, desc='checkbox'),
    noise=dict(type='float', value=3, min=1, max=10, desc='slider'),
    format=dict(type='select', value='jpg', options=['png', 'bmp', 'jpg'], desc=''),
)

@init
def setup(indir, outdir, settings):
    crunchy.add(path=indir, depth=1, clear=False)
    crunchy.add(path=outdir, depth=0, clear=False)
    return True  # False cancels launch

def step_a(data, outpath, settings):
    data['image'] = ...  # share via data; write files to outpath

def step_b(data, outpath, settings):
    ...

@fileTrigger(flow=[step_a, step_b], fail=crunchy.base.errors.logAndStop, vb=1, priority=1)
def process(data, outpath, settings):
    path = data['path']
    if (path / 'point.npy').exists():
        data['input'] = path / 'point.npy'
        return trigger.PROCESS, outpath / path.name
        # or return trigger.PROCESS, outpath / path.name, 10  # per-file priority
    return trigger.REJECT, ''

@fileTrigger(flow=[step_b], fail=crunchy.base.errors.logAndContinue)
def assemble(data, outpath, settings):
    files = list(data['path'].glob('*.image.npy'))
    if settings['assemble'] and len(files) == settings['set_count']:
        data['files'] = files
        return trigger.PROCESS, data['path']
    return trigger.WAIT, ''  # or REJECT

@finish
def final_tasks(indir, outdir, settings):
    print('done')
```

Setting types the GUI understands: `int`, `float`, `string`, `path`, `select`, `bool`.

Ship a sidecar `.ini` next to the workflow and load it after defining `workflow_settings`:

```python
crunchy.read_ini(
    Path(__file__).with_name('myflow.ini'),
    crunchy.crunchy_settings,
    crunchy.workflow_settings,
)
```

Optional dashboard (Jinja, auto-refresh): assign `crunchy.dashboard` to an HTML string that `{% extends 'base.html' %}`. Template extras: `os`, `re`, `glob`, `natsorted`, `inpath`, `outpath`, `root`, `crunchy`.

## 2. Headless run

Must run under `if __name__ == '__main__'` in a script.

```python
import crunchy
import crunchy.workflows.dummy  # registers triggers / settings

crunchy.init(7)
crunchy.settings['wait'] = 1
crunchy.settings['idle'] = 0.1
crunchy.setOutpath(out_dir)
crunchy.setInpath(in_dir)
crunchy.run()          # scout + control thread; returns the Process
# ... wait ...
crunchy.complete()     # drain queue, stop threads, run @finish
```

`setup()` in dummy also creates synthetic `point.npy` inputs and calls `crunchy.add(...)`.

## 3. Flask control UI

```python
import crunchy.workflows.dummy
from crunchy.app import run

if __name__ == '__main__':
    crunchy.debug = True
    crunchy.crunchy_settings['nthreads']['value'] = 5
    run(basepath)   # http://127.0.0.1:5001
```

`basepath` is the root for relative `inpath` / `outpath` and the file browser. Create `CrunchyIn` first (`mustexist=True`).

Repo launchers: `launch.py` (dummy, `Path.home()`) and `crunchy/app/app.py` (hard-coded test paths).

GUI: Launchpad (`settings.html`) then status (`index.html`) with Pause / Resume / Finish / Terminate and optional Dashboard.

## 4. Mirror a directory

```python
from crunchy.base.mirror import mirror
mirror(local_path, remote_path, sleeptime=5.0, maxiter=2, delete=False, debug=True)
```

Blocks until `maxiter`. Copies new or resized files; `delete=True` removes remote orphans.

## 5. Dummy workflow (shipped)

`crunchy.workflows.dummy`: `@init` writes `Set{i}/{name}{j}/point.npy`, scouts `indir` at depth 1 and `outdir` at depth 0.

1. `process` — folder with `point.npy` → `build_image` → `add_noise` → `save_image` (writes `{name}_{pid}.image.npy` and `{name}.image.{fmt}`).
2. `assemble` — once `set_count` `*.image.npy` exist and no `comp.image.png` → `average` + `save_image` as `comp.image.png`.
3. `@finish` prints `A job well done!`.

Full integration: `tests/test_dummy.py::test_dummy` (60 s sleep, 7 workers, expects 3 `*/comp.image.png`).
