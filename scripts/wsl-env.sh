#!/usr/bin/env bash
# Uso (WSL, Bash): na raiz do clone execute:
#   source scripts/wsl-env.sh
#
# Define REPO e AIRFLOW_HOME para a raiz do repositório e ativa o venv em
# ~/.venvs/mo430-data-pipeline (ou o caminho em MO430_VENV).

_scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REPO="$(cd "${_scripts_dir}/.." && pwd)"
export AIRFLOW_HOME="$REPO"

MO430_VENV="${MO430_VENV:-${HOME}/.venvs/mo430-data-pipeline}"
if [[ -f "${MO430_VENV}/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "${MO430_VENV}/bin/activate"
else
  echo "MO430: venv não encontrado em ${MO430_VENV}" >&2
  echo "MO430: na raiz do clone rode (uma vez): bash scripts/bootstrap-wsl-venv.sh" >&2
fi

unset _scripts_dir
