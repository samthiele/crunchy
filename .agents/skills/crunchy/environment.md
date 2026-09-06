# Python environment for crunchy

Run this protocol **before** using crunchy in scripts, tests or notebooks. Do **not** assume the IDE’s default `python` has `crunchy`.

Use one **verified interpreter** for the whole session (record `PYTHON:` from the probe).

From **this skill directory**:

```bash
python scripts/check_env.py
```

From the **crunchy repo root**:

```bash
python .agents/skills/crunchy/scripts/check_env.py
```

| Result | Action |
|--------|--------|
| Exit 0 | Reuse printed `PYTHON:` for the session |
| Exit 1 (`MISSING_DEPS`) | Stop. Ask the user — do not `pip install` without consent |

**Skip the probe** for conceptual Q&A with no code.

A common setup on this machine is the conda/mamba env `hylite` (`mamba run -n hylite python …`). Treat that as an example, not a requirement.

## Install (only if the user asked)

`setup.py` lists `multiprocess` and `Flask`. Dummy workflow and tests need **numpy** and Pillow; the Flask file browser needs natsort.

```bash
pip install -e .    # this clone
```

To run tests or import the **local** tree without a reinstall, prefix `PYTHONPATH` with the repo root (site-packages may otherwise shadow it).

Reuse `PYTHON:` for `pytest tests/` and `demonstration.ipynb`.
