"""Executa callables medalhão em subprocess (evita fork deadlock no macOS)."""

from __future__ import annotations

import os
import shlex
from pathlib import Path

from airflow.operators.bash import BashOperator

from utils.paths import project_root


def medallion_bash_task(task_id: str, module_path: str, callable_name: str) -> BashOperator:
    """Roda ``module_path.callable_name()`` com venv e ``AIRFLOW_HOME`` do projeto."""
    root = project_root()
    venv_python = root / ".venv" / "bin" / "python"
    python_bin = venv_python if venv_python.is_file() else "python3"
    code = f"from {module_path} import {callable_name}; {callable_name}()"
    bash_command = (
        f"set -euo pipefail; "
        f"export AIRFLOW_HOME={shlex.quote(str(root))}; "
        f"export PYTHONPATH={shlex.quote(str(root / 'dags'))}; "
        f"export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES; "
        f"{shlex.quote(str(python_bin))} -c {shlex.quote(code)}"
    )
    return BashOperator(task_id=task_id, bash_command=bash_command)
