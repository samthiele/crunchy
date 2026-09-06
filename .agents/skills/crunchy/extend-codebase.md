# Extend or edit the crunchy codebase

**Goal:** Change crunchy source, tests, Flask templates, or the dummy workflow — not operate a specific processing job.

```
- [ ] Environment probe (`scripts/check_env.py`)
- [ ] Identify scope: core (`__init__.py`), trigger/scout/mirror, Flask, workflow, test
- [ ] Minimal diff; match neighbouring modules
- [ ] Run targeted tests (`pytest tests/test_base.py` first; `test_dummy.py` is slow)
```

## Conventions

- Follow patterns in neighbouring modules; reuse existing helpers.
- Keep code clear and minimal. Avoid private helpers unless used at least twice.
- One short explanatory comment per block of code.
- Public workflow API is the decorators in `crunchy.base.trigger` plus `init` / `run` / `complete`. Do not add a CLI entry point unless asked.
- Use `multiprocess` (not `multiprocessing`) so queued callables pickle with dill.
- Worker jobs go through `PriorityJobQueue` (higher `priority` first). `settings` / `prog` are Manager dicts; scout file maps and logs are in-process (workers send log lines on a Queue).
- Workflow functions write files; do not invent new IPC for results.
- New workflows: a module under `crunchy/workflows/` that sets `workflow_settings` (and optional `dashboard` / `name`) and registers `@init` / `@fileTrigger` / `@finish` on import.
- Flask templates live in `crunchy/app/templates/`; static in `crunchy/app/static/`. Keep settings widgets in sync with types in `settings.html`.
- Use the same `PYTHON:` from the environment probe. For this clone, set `PYTHONPATH` to the repo root or `pip install -e .` so tests import local code.

For API behaviour, read docstrings, [reference.md](reference.md), and `demonstration.ipynb`. Verify against `tests/test_base.py` and `tests/test_dummy.py` before documenting new behaviour in this skill.
