#!/usr/bin/env python3
"""Probe that crunchy (and declared deps) are importable. Exit 0 on success, 1 with MISSING_DEPS."""
import sys

missing = []
for name in ("multiprocess", "flask", "numpy", "crunchy"):
    try:
        __import__(name)
    except ImportError:
        missing.append(name)

if missing:
    print("MISSING_DEPS: " + ", ".join(missing))
    sys.exit(1)

print("SUCCESS: crunchy is available.")
print("PYTHON:", sys.executable)
print("CRUNCHY:", __import__("crunchy").__file__)
