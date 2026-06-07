"""Correlaciona genes de gold_geo_nodes.csv com areas do pathway.html (map05171).

Padrao KEGG no title: K##### (SYMBOL). Ex.: K06496 (SELP)
Regex: \\(SYMBOL\\) com match exato do symbol do gold.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML_PATH = ROOT / "pathwayApp" / "pathway.html"
GOLD_PATH = ROOT / "data" / "gold" / "gold_geo_nodes.csv"
OUTPUT_PATH = ROOT / "pathwayApp" / "gold_pathway_areas.csv"

AREA_TAG_RE = re.compile(r"<area\b(?P<attrs>[^>]*?)>", re.I)
ID_RE = re.compile(r'\bid="([^"]+)"')
TITLE_RE = re.compile(r'\btitle="([^"]+)"')
HREF_RE = re.compile(r'\bhref="/entry/([^"]+)"')


def symbol_title_pattern(symbol: str) -> re.Pattern[str]:
    """Regex funcional: symbol exato dentro de parenteses no title KEGG."""
    return re.compile(r"\(" + re.escape(symbol) + r"\)")


def load_mapdata_areas(html: str) -> list[dict[str, str]]:
    map_start = html.index('<map id="mapdata"')
    map_end = html.index("</map>", map_start)
    map_html = html[map_start:map_end]

    areas: list[dict[str, str]] = []
    for match in AREA_TAG_RE.finditer(map_html):
        attrs = match.group("attrs")
        id_match = ID_RE.search(attrs)
        title_match = TITLE_RE.search(attrs)
        href_match = HREF_RE.search(attrs)
        if not id_match or not title_match:
            continue
        href = href_match.group(1) if href_match else ""
        if not href.startswith("K"):
            continue
        areas.append(
            {
                "idarea": id_match.group(1),
                "title": title_match.group(1),
            }
        )
    return areas


def correlate(gold_rows: list[dict[str, str]], areas: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for gold in gold_rows:
        symbol = gold["symbol"]
        pattern = symbol_title_pattern(symbol)
        for area in areas:
            if not pattern.search(area["title"]):
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "title": area["title"],
                    "pvalue": gold["pvalue"],
                    "log2foldchange": gold["log2foldchange"],
                    "idarea": area["idarea"],
                }
            )
    rows.sort(key=lambda row: (row["symbol"], row["idarea"]))
    return rows


def main() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")
    with GOLD_PATH.open(newline="", encoding="utf-8") as handle:
        gold_rows = list(csv.DictReader(handle))

    areas = load_mapdata_areas(html)
    rows = correlate(gold_rows, areas)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["symbol", "title", "pvalue", "log2foldchange", "idarea"],
        )
        writer.writeheader()
        writer.writerows(rows)

    symbols = sorted({row["symbol"] for row in rows})
    print(f"Areas KEGG (genes): {len(areas)}")
    print(f"Genes gold correlacionados: {len(symbols)} -> {symbols}")
    print(f"Linhas escritas: {len(rows)} -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
