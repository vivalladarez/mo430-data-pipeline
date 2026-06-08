#!/usr/bin/env bash
# Inicia scheduler + webserver com variáveis corretas para o pipeline medalhão.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PATH="${AIRFLOW_VENV_PATH:-${PROJECT_ROOT}/.venv}"

export AIRFLOW_HOME="${AIRFLOW_HOME:-${PROJECT_ROOT}}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-${PROJECT_ROOT}/dags}"
export PYTHONPATH="${PROJECT_ROOT}/dags"
export AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER=True
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:///${AIRFLOW_HOME%/}/airflow.db}"
# macOS: evita deadlock ao executar tasks Python (pandas) apos fork do scheduler.
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

LOG_DIR="${AIRFLOW_HOME%/}/logs/local"
mkdir -p "${LOG_DIR}"

if [[ ! -f "${VENV_PATH}/bin/activate" ]]; then
  echo "Erro: venv nao encontrado em ${VENV_PATH}" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "${VENV_PATH}/bin/activate"

if pgrep -f "${PROJECT_ROOT}/.venv/bin/airflow" >/dev/null 2>&1; then
  echo "Parando instancias Airflow existentes..."
  pkill -TERM -f "${PROJECT_ROOT}/.venv/bin/airflow" 2>/dev/null || true
  sleep 5
  pkill -KILL -f "${PROJECT_ROOT}/.venv/bin/airflow" 2>/dev/null || true
  sleep 2
fi
rm -f "${AIRFLOW_HOME%/}/airflow-webserver.pid"

airflow db migrate >/dev/null

# macOS: webserver gunicorn pode crashar (SIGSEGV); usar scheduler-only por defeito.
if [[ "$(uname -s)" == "Darwin" && -z "${AIRFLOW_DEV_MODE:-}" ]]; then
  AIRFLOW_DEV_MODE="scheduler"
fi

# Modo standalone (Linux ou AIRFLOW_DEV_MODE=standalone).
if [[ "${AIRFLOW_DEV_MODE}" == "standalone" ]]; then
  nohup airflow standalone >>"${LOG_DIR}/standalone.log" 2>&1 &
  sleep 12
  echo "Airflow standalone iniciado."
else
  nohup airflow scheduler >>"${LOG_DIR}/scheduler.log" 2>&1 &
  if [[ "$(uname -s)" != "Darwin" ]]; then
    nohup airflow webserver --port "${AIRFLOW_PORT:-8080}" >>"${LOG_DIR}/webserver.log" 2>&1 &
  else
    echo "Webserver omitido no macOS (gunicorn SIGSEGV). Use CLI ou tasks test."
  fi
  sleep 4
  echo "Airflow scheduler iniciado."
fi

echo "- UI: http://localhost:${AIRFLOW_PORT:-8080}"
echo "- Logs: ${LOG_DIR}"
echo "- Parar: pkill -TERM -f '${PROJECT_ROOT}/.venv/bin/airflow'"
