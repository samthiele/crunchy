---
name: crunchy
description: >-
  File-watching multithreaded processing for realtime workflows — scout
  directories, @fileTrigger / @init / @finish decorators, worker pool,
  Flask control UI on port 5001, dummy workflow, local-to-remote mirror.
  Use when building or running crunchy workflows, watching hot folders,
  launching the crunchy app, or editing this repository. There is no CLI;
  import a workflow module then call the Python API.
license: GPL-2.0
---

# crunchy

Generic platform for asynchronous, multithreaded processing. A scout watches “hot” directories for files or folders whose size has stabilised, then file filters decide whether to queue a chain of workflow functions on a worker pool.

- GitHub: https://github.com/samthiele/crunchy
- Docs: https://samthiele.github.io/crunchy/crunchy.html
- Tutorial: `demonstration.ipynb`
- Literature: [context.md](context.md)

This skill follows the [Agent Skills](https://agentskills.io) format. Prefer live source over this skill if they disagree.

## Workflow

```
Task progress:
- [ ] Step 1: Verify Python environment (if code will run)
- [ ] Step 2: Classify task type
- [ ] Step 3: Follow the task playbook
```

### Step 1: Python environment

Follow **[environment.md](environment.md)**. Probe:

```bash
python scripts/check_env.py
```

From the repo root: `python .agents/skills/crunchy/scripts/check_env.py`.

On `MISSING_DEPS`, stop and ask. **Skip the probe** for conceptual Q&A.

Declared: `multiprocess`, `Flask`. Runtime also needs **numpy** (`scout`). Dummy workflow and GUI extras: Pillow, natsort.

### Step 2: Classify the task

| Type | When | Playbook |
|------|------|----------|
| **Write a workflow** | New `@fileTrigger` / `@init` / `@finish` | [workflows.md](workflows.md) §1 |
| **Headless run** | Script or notebook, no GUI | [workflows.md](workflows.md) §2 |
| **Launch Flask UI** | Settings, pause/finish, dashboard | [workflows.md](workflows.md) §3 |
| **Mirror directories** | Local → remote copy | [workflows.md](workflows.md) §4 |
| **Dummy example** | Shipped `crunchy.workflows.dummy` | [workflows.md](workflows.md) §5 |
| **Extend codebase** | Source, tests, Flask templates | [extend-codebase.md](extend-codebase.md) |

API: [reference.md](reference.md). Tests: `tests/test_base.py`, `tests/test_dummy.py`.

## Core concepts

```
hot dirs  →  scout (stable size)  →  file filters  →  worker queue  →  flow functions
```

**Scout** — `crunchy.add(path, depth)` then `crunchy.scout()` / `crunchy.run()`. `depth=0` lists the directory itself; `depth=N` descends N levels then lists. A path is “new” after `wait` consecutive visits at the same size (`idle` seconds between visits).

**Workflow module** — typically `crunchy/workflows/<name>.py`. Importing it registers:

- `crunchy.workflow_settings` — GUI-editable knobs
- `crunchy.dashboard` — optional Jinja string for `/dashboard`
- `@init` setup, `@fileTrigger` filters, `@finish` teardown

**File filter** — decorated function `(data, outpath, settings) → (status, outpath)` or `(status, outpath, priority)`. `data` starts as `{'path': Path(...)}`. Status:

| Constant | Value | Meaning |
|----------|-------|---------|
| `trigger.ERROR` | -1 | Filter crashed (fail handler) |
| `trigger.REJECT` | 0 | Ignore |
| `trigger.WAIT` | 1 | See again (removed from `known_files`) |
| `trigger.PROCESS` | 2 | Queue `flow` on a worker |

**Flow functions** — `(data, outpath, settings)`. Share state via `data`; write results to disk (not return values). A second filter can watch the output tree to assemble later products.

**Workers** — `multiprocess.Process` (not stdlib `multiprocessing`). Scout and the control loop are in-process **threads**. `init(n)` then `run()` starts them. Jobs go on a priority queue (`fileTrigger(..., priority=N)`; higher N first). `complete(join=True, end=True)` resumes if paused, drains the queue, stops threads, runs `@finish`, then shuts down the Manager.

## Conventions and pitfalls

- Import the workflow **before** `init()` / `run()` / `from crunchy.app import run`. Importing a workflow registers `@finish` globally — any later `complete()` will run it (this used to break `test_workers` when `pytest tests/` collected `test_dummy.py`).
- Call `init()` and Flask `run()` inside `if __name__ == '__main__'` (Windows spawn / Flask duplication).
- Re-running `@init` or `@fileTrigger` in a notebook registers **another** handler — do not re-execute those cells.
- `wait` is visit count, not seconds. Stability delay ≈ `wait * idle` (one sleep per full scan of all hot dirs).
- `@fileTrigger(..., priority=0)` — higher values run first. A filter may return `(PROCESS, outpath, priority)` to override per file.
- `ram_reserve` (default `4` GB) pauses new jobs when free RAM is below that many gigabytes. Set `0` to disable. `complete()` still drains.
- Defaults: `crunchy/crunchy.ini`. A `crunchy.ini` / `crunchy.json` in the cwd (or `$CRUNCHY_CONFIG`) overrides it. Matching keys also override `workflow_settings`. Workflows can ship a sidecar `.ini` and call `crunchy.read_ini(path, crunchy.crunchy_settings, crunchy.workflow_settings)`.
- Job exceptions are logged to `logs/YYYY-MM-DD/errors.txt` (override `settings['error_log']`). All thread log lines go to `logs/YYYY-MM-DD/log.txt`. The Errors tab shows only today. A crashed process is restarted by a watcher thread.
- Default `inpath` is `CrunchyIn` with `mustexist=True` — create it before launching the GUI.
- Flask listens on **5001** (`0.0.0.0`); falls back to a random port if busy. Header: Home / Settings / Directories / Files / Errors, plus a theme toggle (dark by default; `localStorage`). Launchpad values (inpath, outpath, …) are stored in a `crunchy-prefs` cookie, **one slot per workflow**, so dummy vs sensor jobs do not overwrite each other. `/errors` shows today's `logs/YYYY-MM-DD/errors.txt`. The home page shows free disk on the outpath volume. Directories → **Watch this folder** calls `watch()`. Files lists scout/filter/queue state; ignored paths can be **Recheck**ed (`recheck()`).
- There is no setuptools console script. Use `launch.py` (opens the default browser) or `from crunchy.app import run`. `run(basepath, open_browser=True)` uses `webbrowser.open`.
- Shipped example workflow is `dummy`. Dummy `build_image` / `add_noise` use nested Python loops (intentionally slow). `test_dummy` sleeps 60 s and can take many minutes.
- `test_scout` asserts hard-coded `.npy` byte sizes (928 / 228) — numpy header changes can fail it.
- `/debug` in the Flask app returns 404.
- `mirror(..., delete=True)` removes remote files missing locally — dangerous.

## Resources

- Recipes from `demonstration.ipynb` / tests: [workflows.md](workflows.md)
- API, settings schema, Flask routes: [reference.md](reference.md)
- Literature: [context.md](context.md)
- Editing this repo: [extend-codebase.md](extend-codebase.md)
- Environment: [environment.md](environment.md)
