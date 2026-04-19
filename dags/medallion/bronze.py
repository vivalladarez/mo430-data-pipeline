"""Camada bronze: leitura de fonte bruta e persistência sem transformação de negócio."""

from __future__ import annotations

import csv
from datetime import datetime, timezone

from medallion.paths import data_dir


def run_bronze(**_context) -> None:
    raw_path = data_dir() / "raw" / "sample.csv"
    out_dir = data_dir() / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "bronze_ingest.csv"

    if not raw_path.is_file():
        raise FileNotFoundError(f"Arquivo bruto não encontrado: {raw_path}")

    ingested_at = datetime.now(timezone.utc).isoformat()
    with raw_path.open(newline="", encoding="utf-8") as src, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or []) + ["ingested_at"]
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            row["ingested_at"] = ingested_at
            writer.writerow(row)
