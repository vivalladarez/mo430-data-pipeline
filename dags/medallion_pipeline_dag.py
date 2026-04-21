"""
DAG exemplo medalhão: bronze → silver (GEO e EBI em paralelo) → gold mock 1:1.

Cada etapa lê/escreve ficheiros em ``data/`` sob AIRFLOW_HOME.
O mock gold copia os CSV silver para ``data/gold/gold_mock_*.csv`` (ver também
``notebooks/gold_mock_1to1.ipynb``).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from medallion.bronze.bronze import run_bronze
from medallion.bronze.bronze_ebi import run_bronze_ebi
from medallion.gold.gold import run_gold_mock_1to1
from medallion.gold.gold_edge_ppi import run_gold_edge_ppi
from medallion.silver.silver import run_silver_ebi, run_silver_geo

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
    silver_geo = PythonOperator(
        task_id="silver_geo",
        python_callable=run_silver_geo,
    )
    silver_ebi = PythonOperator(
        task_id="silver_ebi",
        python_callable=run_silver_ebi,
    )
    gold_mock_1to1 = PythonOperator(
        task_id="gold_mock_1to1",
        python_callable=run_gold_mock_1to1,
    )
    gold_edge_ppi = PythonOperator(
        task_id="gold_edge_ppi",
        python_callable=run_gold_edge_ppi,
    )

    bronze_geo_soft_ingest >> silver_geo
    silver_geo >> gold_mock_1to1
    silver_geo >> gold_edge_ppi
    bronze_ebi_gxa_ingest >> silver_ebi >> gold_mock_1to1
