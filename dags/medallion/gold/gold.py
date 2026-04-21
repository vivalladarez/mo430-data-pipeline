"""Camada gold: leitura da silver e agregação para consumo analítico."""

from __future__ import annotations

import pandas as pd

from medallion.silver.silver import (
    SILVER_GEO_NODES_CSV,
    SILVER_GEO_NODES_PRINCIPAL_CSV,
)
from utils.paths import data_dir

GOLD_GEO_NODES_CSV = "gold_geo_nodes.csv"


def run_gold_geo_nodes(**_context) -> None:
    """Gold GEO: cruza ``silver_geo_nodes`` com ``silver_geo_nodes_principal`` por ``geneid``.

    Mantém apenas genes presentes em NOS (inner join). Grava
    ``data/gold/gold_geo_nodes.csv``.

    Colunas que existem nos dois CSVs ficam só com os valores do GSE; do NOS
    entram apenas colunas que não existem no GSE (ex.: ``neg_log10_pvalue``,
    ``dataset_id``), sem sufixos nem prefixos.
    """
    silver_dir = data_dir() / "silver"
    gold_dir = data_dir() / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    geo_path = silver_dir / SILVER_GEO_NODES_CSV
    nos_path = silver_dir / SILVER_GEO_NODES_PRINCIPAL_CSV
    out_path = gold_dir / GOLD_GEO_NODES_CSV

    if not geo_path.is_file():
        raise FileNotFoundError(f"Silver GEO nao encontrado: {geo_path}")
    if not nos_path.is_file():
        raise FileNotFoundError(f"Silver geo nos nodes nao encontrado: {nos_path}")

    geo = pd.read_csv(geo_path)
    nos = pd.read_csv(nos_path)
    if "geneid" not in geo.columns or "geneid" not in nos.columns:
        raise ValueError("Ambos os CSVs precisam da coluna 'geneid' para o cruzamento.")

    geo = geo.copy()
    nos = nos.copy()
    geo["geneid"] = pd.to_numeric(geo["geneid"], errors="coerce").astype("Int64")
    nos["geneid"] = pd.to_numeric(nos["geneid"], errors="coerce").astype("Int64")

    nos_only_cols = [c for c in nos.columns if c not in geo.columns]
    nos_add = nos[["geneid", *nos_only_cols]]
    merged = geo.merge(nos_add, on="geneid", how="inner")
    merged.to_csv(out_path, index=False)
