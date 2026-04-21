"""
DAG exemplo medalhão: bronze → silver (GEO GSE, GEO NOS, EBI) → gold (só ``gold_geo_nodes``).

Cada etapa lê/escreve ficheiros em ``data/`` sob AIRFLOW_HOME.
A única task gold cruza ``silver_geo_nodes`` com ``silver_geo_nodes_principal`` (ver
``dags/medallion/gold/gold_geo_nodes.ipynb``).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medallion.bronze.bronze import run_bronze
from medallion.bronze.bronze_ebi import run_bronze_ebi
from medallion.gold.gold import run_gold_geo_nodes
from medallion.silver.silver import (
    run_silver_ebi_nodes,
    run_silver_geo_nodes,
    run_silver_geo_nodes_principal,
)

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
    silver_geo_nodes = PythonOperator(
        task_id="silver_geo_nodes",
        python_callable=run_silver_geo_nodes,
    )
    silver_geo_nodes_principal = PythonOperator(
        task_id="silver_geo_nodes_principal",
        python_callable=run_silver_geo_nodes_principal,
    )
    silver_ebi_nodes = PythonOperator(
        task_id="silver_ebi_nodes",
        python_callable=run_silver_ebi_nodes,
    )
    gold_geo_nodes = PythonOperator(
        task_id="gold_geo_nodes",
        python_callable=run_gold_geo_nodes,
    )

    bronze_geo_soft_ingest >> silver_geo_nodes
    bronze_geo_soft_ingest >> silver_geo_nodes_principal
    [silver_geo_nodes, silver_geo_nodes_principal] >> gold_geo_nodes
    bronze_ebi_gxa_ingest >> silver_ebi_nodes
