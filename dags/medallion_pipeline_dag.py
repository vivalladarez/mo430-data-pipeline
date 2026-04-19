"""
DAG exemplo medalhão: bronze → silver → gold.

Cada etapa lê/escreve arquivos CSV em ``data/`` sob AIRFLOW_HOME.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medallion.bronze import run_bronze
from medallion.gold import run_gold
from medallion.silver import run_silver

with DAG(
    dag_id="medallion_sample_pipeline",
    description="Pipeline medalhão de exemplo (CSV local)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mo430", "medalhao", "exemplo"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze,
    )
    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver,
    )
    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold,
    )

    bronze >> silver >> gold
