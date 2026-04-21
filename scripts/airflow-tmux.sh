#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SESSION_NAME="${AIRFLOW_TMUX_SESSION:-mo430-airflow}"
AIRFLOW_PORT="${AIRFLOW_PORT:-8080}"
VENV_PATH="${AIRFLOW_VENV_PATH:-${PROJECT_ROOT}/.venv}"
AIRFLOW_HOME="${AIRFLOW_HOME:-${PROJECT_ROOT}}"
AIRFLOW_DATA_DIR="${AIRFLOW_DATA_DIR:-${AIRFLOW_HOME%/}/data}"

export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-${PROJECT_ROOT}/dags}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:///${AIRFLOW_HOME%/}/airflow.db}"
export AIRFLOW__WEBSERVER__WORKERS="${AIRFLOW__WEBSERVER__WORKERS:-1}"
export AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE="${AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE:-True}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-False}"
export AIRFLOW_DATA_DIR

LOG_DIR="${AIRFLOW_HOME%/}/logs/local"
SCHEDULER_LOG="${LOG_DIR}/scheduler.log"
WEBSERVER_LOG="${LOG_DIR}/webserver.log"

usage() {
  cat <<EOF
Uso: $(basename "$0") <comando>

Comandos:
  start     Inicia scheduler e webserver no tmux
  down      Para a sessao tmux

Variaveis opcionais:
  AIRFLOW_TMUX_SESSION            (default: ${SESSION_NAME})
  AIRFLOW_PORT                    (default: ${AIRFLOW_PORT})
  AIRFLOW_VENV_PATH               (default: ${VENV_PATH})
  AIRFLOW_HOME                    (default: ${AIRFLOW_HOME})
  AIRFLOW_DATA_DIR                (default: ${AIRFLOW_DATA_DIR})
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Erro: comando obrigatorio nao encontrado: $1" >&2
    exit 1
  fi
}

check_prereqs() {
  require_cmd tmux
  require_cmd python3
  if [[ ! -f "${VENV_PATH}/bin/activate" ]]; then
    echo "Erro: venv nao encontrado em ${VENV_PATH}" >&2
    echo "Crie o venv (ex.: python3 -m venv .venv) e instale requirements." >&2
    exit 1
  fi
}

shell_cmd() {
  local role="$1"
  local logfile="$2"
  cat <<EOF
cd "${PROJECT_ROOT}" && \
source "${VENV_PATH}/bin/activate" && \
export AIRFLOW_HOME="${AIRFLOW_HOME}" && \
export AIRFLOW_DATA_DIR="${AIRFLOW_DATA_DIR}" && \
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER}" && \
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN}" && \
export AIRFLOW__WEBSERVER__WORKERS="${AIRFLOW__WEBSERVER__WORKERS}" && \
export AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE="${AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE}" && \
mkdir -p "${LOG_DIR}" && \
echo "[\$(date -Iseconds)] Starting ${role}" | tee -a "${logfile}" && \
airflow ${role} $( [[ "${role}" == "webserver" ]] && echo "--port ${AIRFLOW_PORT}" ) 2>&1 | tee -a "${logfile}"
EOF
}

start_session() {
  check_prereqs
  mkdir -p "${LOG_DIR}"

  if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
    echo "Sessao tmux '${SESSION_NAME}' ja existe."
    echo "Use: $(basename "$0") down"
    exit 0
  fi

  (
    source "${VENV_PATH}/bin/activate"
    airflow db migrate >/dev/null
  )

  rm -f "${AIRFLOW_HOME%/}/airflow-webserver.pid"

  tmux new-session -d -s "${SESSION_NAME}" -n scheduler
  tmux send-keys -t "${SESSION_NAME}:scheduler" "$(shell_cmd "scheduler" "${SCHEDULER_LOG}")" C-m

  tmux new-window -t "${SESSION_NAME}" -n webserver
  tmux send-keys -t "${SESSION_NAME}:webserver" "$(shell_cmd "webserver" "${WEBSERVER_LOG}")" C-m

  echo "Sessao '${SESSION_NAME}' iniciada."
  echo "- Web UI: http://localhost:${AIRFLOW_PORT}"
  echo "- Logs: ${LOG_DIR}"
  echo "- Para parar: $(basename "$0") down"
}

down_session() {
  if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
    tmux kill-session -t "${SESSION_NAME}"
    echo "Sessao '${SESSION_NAME}' finalizada."
  else
    echo "Sessao '${SESSION_NAME}' nao esta ativa."
  fi
}

cmd="${1:-}"
case "${cmd}" in
  start) start_session ;;
  down) down_session ;;
  -h|--help|help|"") usage ;;
  *) echo "Comando invalido: ${cmd}" >&2; usage; exit 1 ;;
esac
