"""Configuração local carregada pelo Airflow em scheduler, webserver e tasks."""

from __future__ import annotations

import os
import sys

# macOS + pandas: evita deadlock quando o executor faz fork de processos.
os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")

_airflow_home = os.environ.get("AIRFLOW_HOME", os.getcwd())
_dags_dir = os.path.join(_airflow_home, "dags")
if os.path.isdir(_dags_dir) and _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)
