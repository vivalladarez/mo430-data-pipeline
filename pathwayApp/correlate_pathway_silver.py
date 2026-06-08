"""Correlaciona genes de silver_geo_nodes.csv com areas do pathway.html (map05171).

Mesmo formato de gold_pathway_areas.csv: symbol, title, pvalue, log2foldchange, idarea.
Regex no title KEGG: \\(SYMBOL\\) — ex.: K06496 (SELP)
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

from correlate_pathway_gold import correlate, load_mapdata_areas, symbol_title_pattern

ROOT = Path(__file__).resolve().parents[1]
HTML_PATH = ROOT / "pathwayApp" / "pathway.html"
SILVER_PATH = ROOT / "data" / "silver" / "silver_geo_nodes.csv"
OUTPUT_PATH = ROOT / "pathwayApp" / "silver_pathway_areas.csv"


def _safe_pvalue(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def load_silver_by_symbol(path: Path) -> list[dict[str, str]]:
    """Um registro por symbol; em duplicatas, mantem o menor pvalue."""
    best: dict[str, dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            symbol = (row.get("symbol") or "").strip()
            if not symbol:
                continue
            current = best.get(symbol)
            if current is None or _safe_pvalue(row["pvalue"]) < _safe_pvalue(current["pvalue"]):
                best[symbol] = row
    return list(best.values())


def main() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")
    silver_rows = load_silver_by_symbol(SILVER_PATH)
    areas = load_mapdata_areas(html)
    rows = correlate(silver_rows, areas)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "title", "pvalue", "log2foldchange", "idarea"],
        )
        writer.writeheader()
        writer.writerows(rows)

    symbols = sorted({row["symbol"] for row in rows})
    print(f"Simbolos silver (unicos): {len(silver_rows)}")
    print(f"Areas KEGG (genes): {len(areas)}")
    print(f"Genes silver correlacionados: {len(symbols)}")
    print(f"Linhas escritas: {len(rows)} -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
