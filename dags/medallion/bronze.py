"""Camada bronze: leitura de fonte bruta e persistência sem transformação de negócio."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from medallion.soft_file_parser import parse_soft_file
    from medallion.paths import data_dir
except ModuleNotFoundError:
    # Fallback para execução direta: `python3 dags/medallion/bronze.py`
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from dags.medallion.soft_file_parser import parse_soft_file
    from dags.medallion.paths import data_dir

def run_bronze(**_context) -> None:
    raw_dir = data_dir() / "raw"
    out_dir = data_dir() / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)

    files = list(raw_dir.glob("*.soft.gz"))

    if not files:
        raise FileNotFoundError(f"Nenhum arquivo bruto encontrado em: {raw_dir}")

    for file in files:
        df = parse_soft_file(file)
        df["ingested_at"] = datetime.now(timezone.utc).isoformat()
        out_path = out_dir / f"{file.stem}.csv"
        df.to_csv(out_path, index=False)
