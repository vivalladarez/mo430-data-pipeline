#!/usr/bin/env bash
# Executa o pipeline medalhão em sequência (sem fork do scheduler). Útil no macOS.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV="${ROOT}/.venv/bin/python"

export AIRFLOW_HOME="${ROOT}"
export PYTHONPATH="${ROOT}/dags"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

run() {
  echo ">> $1"
  "${VENV}" -c "from ${2} import ${3}; ${3}()"
}

run "bronze GEO" medallion.bronze.bronze run_bronze
run "bronze EBI" medallion.bronze.bronze_ebi run_bronze_ebi
run "silver GEO" medallion.silver.silver run_silver_geo_nodes
run "silver GEO principal" medallion.silver.silver run_silver_geo_nodes_principal
run "silver EBI" medallion.silver.silver run_silver_ebi_nodes
run "silver Open Targets" medallion.silver.silver_opentargets_clinical run_silver_opentargets_clinical
run "gold GEO" medallion.gold.gold run_gold_geo_nodes
run "gold PPI" medallion.gold.gold_edge_ppi run_gold_edge_ppi
run "gold Open Targets" medallion.gold.gold_opentargets_clinical run_gold_opentargets_clinical

echo "Pipeline medalhão concluído."
