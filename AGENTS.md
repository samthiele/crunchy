# Agent instructions

This repository ships an [Agent Skill](https://agentskills.io) for crunchy at:

`.agents/skills/crunchy/`

When the task involves crunchy, file-scout workflows, worker-thread processing, the Flask control UI, or changes to this codebase, read `.agents/skills/crunchy/SKILL.md` and follow it.

Do not assume the default `python` has crunchy. Run `scripts/check_env.py` from that skill directory before executing code.

Primary tutorial: `demonstration.ipynb`. Tests: `pytest tests/`. Docs: https://samthiele.github.io/crunchy/crunchy.html

Launch (no console script): import a workflow, then `from crunchy.app import run; run(basepath)`.

## Ecosystem

hylite is the core library. Related packages:

| Package | Role | Repository |
|---------|------|------------|
| **hylite** | Load, correct, project, and analyse hyperspectral data | https://github.com/hifexplo/hylite — docs: https://hifexplo.github.io/hylite/hylite.html |
| **hklearn** | Multi-sensor hyperspectral ML (`Stack`, `ModelSet`) | https://github.com/samthiele/hklearn |
| **hycore** | Drillcore data organisation and mosaics | https://github.com/samthiele/hycore |
| **hywiz** | Viewer for hycore hyperspectral core sheds | https://github.com/samthiele/hywiz |
| **ispec** | Interactive spectral libraries, mixing, feature ID | https://github.com/samthiele/ispec |
| **crunchy** | Multithreading for realtime processing workflows | https://github.com/samthiele/crunchy |
| **napari-hippo** | napari UI for some hylite tools | https://github.com/samthiele/napari-hippo |
| **speedy** | Browser-based hyperspectral image viewer (ENVI, overlays) | https://github.com/samthiele/speedy |
