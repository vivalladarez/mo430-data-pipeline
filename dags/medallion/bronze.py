"""Camada bronze: leitura de fonte bruta e persistência sem transformação de negócio."""

from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from medallion.parse_soft_file import parse_soft_file
    from medallion.paths import data_dir
except ModuleNotFoundError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from dags.medallion.parse_soft_file import parse_soft_file
    from dags.medallion.paths import data_dir

def run_bronze(**_context) -> None:
    raw_dir = data_dir() / "raw"
    raw_file_extension = ".soft.gz"
    out_dir = data_dir() / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)

    files = list(raw_dir.glob(f"*{raw_file_extension}"))

    if not files:
        raise FileNotFoundError(f"Nenhum arquivo bruto encontrado em: {raw_dir}")

    for file in files:
        df = parse_soft_file(file)
        df["ingested_at"] = datetime.now(timezone.utc).isoformat()
        file_name = file.name.split(f'{raw_file_extension}')[0]
        out_path = out_dir / f"bronze_{file_name}.csv"
        df.to_csv(out_path, index=False)
