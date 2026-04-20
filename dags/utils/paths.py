"""Caminhos baseados em AIRFLOW_HOME (raiz do projeto)."""

from __future__ import annotations

import os
from pathlib import Path


def project_root() -> Path:
    return Path(os.environ.get("AIRFLOW_HOME", ".")).resolve()


def data_dir() -> Path:
    return project_root() / "data"
