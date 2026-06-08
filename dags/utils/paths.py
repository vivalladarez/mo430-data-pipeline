"""Caminhos baseados em AIRFLOW_HOME (raiz do projeto)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

# macOS: pandas/objc apos fork do executor do Airflow.
os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")

_airflow_home = os.environ.get("AIRFLOW_HOME")
if _airflow_home:
    _dags_dir = Path(_airflow_home) / "dags"
    if _dags_dir.is_dir():
        _dags_str = str(_dags_dir.resolve())
        if _dags_str not in sys.path:
            sys.path.insert(0, _dags_str)


def project_root() -> Path:
    return Path(os.environ.get("AIRFLOW_HOME", ".")).resolve()


def data_dir() -> Path:
    return project_root() / "data"
