#!/usr/bin/env bash
# Marca runs/tasks presos como failed (SQLite local). Use antes de re-disparar o DAG.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${AIRFLOW_HOME:-${SCRIPT_DIR}/..}/airflow.db"

sqlite3 "${DB}" <<'SQL'
UPDATE dag_run SET state='failed' WHERE state IN ('running','queued');
UPDATE task_instance SET state='failed' WHERE state IN ('running','queued','scheduled');
SQL

echo "Runs/tasks em running/queued foram marcados como failed em ${DB}"
