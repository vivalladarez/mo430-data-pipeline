"""Camada silver: leitura da bronze, limpeza leve e modelo mais confiável."""

from __future__ import annotations

import pandas as pd

from utils.data_cleaners import clean_ebi_expression_dataset, clean_geo_gene_dataset
from utils.paths import data_dir

SILVER_GEO_CSV = "silver_geo.csv"
SILVER_EBI_CSV = "silver_ebi.csv"


def run_silver_geo(**_context) -> None:
    """Consolida bronze GEO (CSVs tabulares por gene) em ``data/silver/silver_geo.csv``."""
    bronze_dir = data_dir() / "bronze"
    silver_dir = data_dir() / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    if not bronze_dir.is_dir():
        raise FileNotFoundError(f"Diretorio bronze nao encontrado: {bronze_dir}")

    geo_bronze = sorted(
        file
        for file in bronze_dir.glob("bronze_GSE*.csv")
        if not file.name.endswith("_family.csv")
    )
    if not geo_bronze:
        raise FileNotFoundError(
            f"Nenhum bronze GEO encontrado em {bronze_dir} (esperado bronze_GSE*.csv)"
        )

    parts: list[pd.DataFrame] = []
    for file in geo_bronze:
        df = pd.read_csv(file)
        df_clean = clean_geo_gene_dataset(df)
        if df_clean.empty:
            raise ValueError(f"Arquivo sem linhas validas apos limpeza: {file.name}")
        df_clean = df_clean.copy()
        df_clean["bronze_source_file"] = file.name
        df_clean["series_id"] = file.stem.replace("bronze_", "", 1)
        parts.append(df_clean)
    geo_silver = pd.concat(parts, ignore_index=True)
    geo_silver.to_csv(silver_dir / SILVER_GEO_CSV, index=False)


def run_silver_ebi(**_context) -> None:
    """Transforma bronze EBI em ``data/silver/silver_ebi.csv``."""
    bronze_dir = data_dir() / "bronze"
    silver_dir = data_dir() / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    ebi_bronze = bronze_dir / "bronze_ebi_expression.csv"
    if not ebi_bronze.is_file():
        raise FileNotFoundError(
            f"Arquivo bronze EBI nao encontrado: {ebi_bronze} (necessario para silver EBI)"
        )

    df = pd.read_csv(ebi_bronze)
    df_clean = clean_ebi_expression_dataset(df)
    if df_clean.empty:
        raise ValueError("Arquivo EBI sem linhas validas apos limpeza")
    df_clean.to_csv(silver_dir / SILVER_EBI_CSV, index=False)
