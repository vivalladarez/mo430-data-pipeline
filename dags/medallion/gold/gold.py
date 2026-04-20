"""Camada gold: leitura da silver e agregação para consumo analítico."""

from __future__ import annotations

import csv
import shutil
from collections import defaultdict

from medallion.silver.silver import SILVER_EBI_CSV, SILVER_GEO_CSV
from utils.paths import data_dir


def run_gold_mock_1to1(**_context) -> None:
    """Copia silver → gold sem agregação (mock 1:1), um CSV por origem.

    Usa ``copyfile`` em vez de ``copy2``: em mounts (WSL, CIFS, OneDrive, etc.)
    ``copystat``/``utime`` frequentemente falha com ``PermissionError``.
    """
    silver_dir = data_dir() / "silver"
    gold_dir = data_dir() / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    pairs: list[tuple[str, str]] = [
        (SILVER_GEO_CSV, "gold_mock_geo.csv"),
        (SILVER_EBI_CSV, "gold_mock_ebi.csv"),
    ]
    copied = 0
    for silver_name, gold_name in pairs:
        src = silver_dir / silver_name
        if src.is_file():
            shutil.copyfile(src, gold_dir / gold_name)
            copied += 1
    if copied == 0:
        raise FileNotFoundError(
            f"Nenhum ficheiro silver encontrado em {silver_dir} "
            f"(esperado pelo menos um de: {SILVER_GEO_CSV}, {SILVER_EBI_CSV})"
        )


def run_gold(**_context) -> None:
    silver_path = data_dir() / "silver" / "silver_clean.csv"
    gold_dir = data_dir() / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / "gold_por_categoria.csv"

    if not silver_path.is_file():
        raise FileNotFoundError(f"Arquivo silver não encontrado: {silver_path}")

    totals: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)

    with silver_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat = (row.get("categoria") or "").strip() or "UNKNOWN"
            try:
                totals[cat] += float(row.get("valor", "0"))
            except ValueError:
                pass
            counts[cat] += 1

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["categoria", "total_valor", "qtd_linhas"]
        )
        writer.writeheader()
        for cat in sorted(totals.keys()):
            writer.writerow(
                {
                    "categoria": cat,
                    "total_valor": f"{totals[cat]:.2f}",
                    "qtd_linhas": str(counts[cat]),
                }
            )
