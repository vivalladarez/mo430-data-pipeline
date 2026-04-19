"""Camada silver: leitura da bronze, limpeza leve e modelo mais confiável."""

from __future__ import annotations

import csv
from pathlib import Path

from medallion.paths import data_dir


def _latest_bronze_file(bronze_dir: Path) -> Path:
    candidates = sorted(bronze_dir.glob("bronze_ingest.csv"))
    if not candidates:
        raise FileNotFoundError(f"Nenhum arquivo bronze em {bronze_dir}")
    return candidates[-1]


def run_silver(**_context) -> None:
    bronze_dir = data_dir() / "bronze"
    silver_dir = data_dir() / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    out_path = silver_dir / "silver_clean.csv"

    bronze_path = _latest_bronze_file(bronze_dir)
    seen_ids: set[str] = set()
    rows_out: list[dict[str, str]] = []

    with bronze_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_id = (row.get("id") or "").strip()
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            row["categoria"] = (row.get("categoria") or "").strip()
            row["observacao"] = (row.get("observacao") or "").strip()
            try:
                row["valor"] = f"{float(row.get('valor', '0')):.2f}"
            except ValueError:
                row["valor"] = "0.00"
            rows_out.append(
                {
                    "id": row_id,
                    "categoria": row["categoria"],
                    "valor": row["valor"],
                    "observacao": row["observacao"],
                    "ingested_at": (row.get("ingested_at") or "").strip(),
                }
            )

    with out_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["id", "categoria", "valor", "observacao", "ingested_at"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)
