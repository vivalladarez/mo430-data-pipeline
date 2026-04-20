"""
DAG exemplo medalhão: bronze → silver → gold.

Cada etapa lê/escreve ficheiros em ``data/`` sob AIRFLOW_HOME.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medallion.bronze.bronze import run_bronze
from medallion.bronze.bronze_ebi import run_bronze_ebi
from medallion.gold.gold import run_gold
from medallion.silver.silver import run_silver

with DAG(
    dag_id="medallion_sample_pipeline",
    description="Pipeline medalhão de exemplo (CSV local)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mo430", "medalhao", "exemplo"],
) as dag:
    bronze_geo_soft_ingest = PythonOperator(
        task_id="bronze_geo_soft_ingest",
        python_callable=run_bronze,
    )
    bronze_ebi_gxa_ingest = PythonOperator(
        task_id="bronze_ebi_gxa_ingest",
        python_callable=run_bronze_ebi,
    )
    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver,
    )
    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold,
    )

    bronze_geo_soft_ingest >> silver >> gold
    bronze_ebi_gxa_ingest >> silver
