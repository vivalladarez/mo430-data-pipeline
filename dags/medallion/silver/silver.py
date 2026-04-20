"""Camada silver: leitura da bronze, limpeza leve e modelo mais confiável."""

from __future__ import annotations

import pandas as pd

from utils.data_cleaners import clean_geo_dataset
from utils.paths import data_dir


def run_silver(**_context) -> None:
    bronze_dir = data_dir() / "bronze"
    silver_dir = data_dir() / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(bronze_dir.glob("bronze_*.csv"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo bronze encontrado")

    for file in files:
        df = pd.read_csv(file)
        df_clean = clean_geo_dataset(df)
        if df_clean.empty:
            raise ValueError(f"Arquivo sem linhas validas apos limpeza: {file.name}")
        file_name = file.stem.replace("bronze_", "silver_", 1)
        out_path = silver_dir / f"{file_name}.csv"
        df_clean.to_csv(out_path, index=False)
