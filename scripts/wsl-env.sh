#!/usr/bin/env bash
# Uso (WSL, Bash): na raiz do clone execute:
#   source scripts/wsl-env.sh
#
# REPO = raiz do Git (ex.: /mnt/c/...).
# AIRFLOW_HOME = pasta em disco Linux (ext4), por defeito ~/mo430-airflow-runtime:
#   airflow.db, logs/, .pid e chmod do Gunicorn falham em /mnt/c/ (OneDrive).
# Ligacoes simbolicas: dags/, data/, plugins/, include/ -> REPO.

_scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REPO="$(cd "${_scripts_dir}/.." && pwd)"

_runtime="${MO430_AIRFLOW_RUNTIME:-${HOME}/mo430-airflow-runtime}"
export AIRFLOW_HOME="${MO430_AIRFLOW_HOME:-${_runtime}}"
mkdir -p "${AIRFLOW_HOME}"

if [[ "${AIRFLOW_HOME}" != "${REPO}" ]]; then
  for _d in dags data plugins include; do
    if [[ -d "${REPO}/${_d}" ]]; then
      ln -sfn "${REPO}/${_d}" "${AIRFLOW_HOME}/${_d}"
    fi
  done
fi

MO430_VENV="${MO430_VENV:-${HOME}/.venvs/mo430-data-pipeline}"
if [[ -f "${MO430_VENV}/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "${MO430_VENV}/bin/activate"
else
  echo "MO430: venv não encontrado em ${MO430_VENV}" >&2
  echo "MO430: na raiz do clone rode (uma vez): bash scripts/bootstrap-wsl-venv.sh" >&2
fi

unset _scripts_dir _d _runtime
