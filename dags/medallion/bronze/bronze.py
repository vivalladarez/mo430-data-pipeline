"""Camada bronze: leitura de fonte bruta e persistência sem transformação de negócio."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from utils.paths import data_dir


def run_bronze(**_context) -> None:
    """Ingestão GEO a partir de CSVs brutos em ``data/raw/geo``.

    Por enquanto, assume que os ficheiros GEO brutos já estão em CSV e devem ser
    apenas persistidos na bronze com o metadado ``ingested_at``.
    """

    raw_dir = data_dir() / "raw" / "geo"
    out_dir = data_dir() / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo bruto encontrado em: {raw_dir}")

    ingested_at = datetime.now(timezone.utc).isoformat()
    for file in files:
        df = pd.read_csv(file)
        df["ingested_at"] = ingested_at
        out_path = out_dir / f"bronze_{file.stem}.csv"
        df.to_csv(out_path, index=False)
