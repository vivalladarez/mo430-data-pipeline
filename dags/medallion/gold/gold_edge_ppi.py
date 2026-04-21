"""Camada gold: construção de arestas PPI via STRING API."""

from __future__ import annotations

import csv
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests import HTTPError, RequestException, Timeout

from medallion.silver.silver import SILVER_GEO_NODES_PRINCIPAL_CSV
from utils.paths import data_dir

LOGGER = logging.getLogger(__name__)

STRING_API_BASE = "https://string-db.org/api/json"
STRING_SPECIES_HUMAN = 9606
DEFAULT_REQUIRED_SCORE = 400
REQUEST_TIMEOUT: tuple[float, float] = (10.0, 45.0)
ID_CHUNK_SIZE = 250
NETWORK_CHUNK_SIZE = 80
REQUEST_PAUSE_SECONDS = 0.2

OUTPUT_COLUMNS = [
    "geneid",
    "node2",
    "node1_string_id",
    "node2_string_id",
    "experimentally_determined_interaction",
    "database_annotated",
    "automated_textmining",
    "combined_score",
]


@dataclass(frozen=True)
class GeneEntry:
    geneid: str
    symbol: str


@dataclass(frozen=True)
class ResolvedGene:
    geneid: str
    symbol: str
    string_id: str
    preferred_name: str
    resolved_by: str


def _chunks[T](items: list[T], chunk_size: int) -> list[list[T]]:
    return [items[idx : idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_geneid(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        as_float = float(raw)
        if as_float.is_integer():
            return str(int(as_float))
    except ValueError:
        return raw
    return raw


def _read_input_genes(silver_path: Path) -> list[GeneEntry]:
    if not silver_path.is_file():
        raise FileNotFoundError(f"Arquivo silver nao encontrado: {silver_path}")

    entries_by_geneid: dict[str, GeneEntry] = {}
    with silver_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV invalido ou sem cabecalho: {silver_path}")
        expected = {"geneid", "symbol"}
        missing = expected.difference(set(reader.fieldnames))
        if missing:
            raise ValueError(
                f"CSV silver sem colunas obrigatorias {sorted(expected)}: faltando {sorted(missing)}"
            )

        for row in reader:
            geneid = _normalize_geneid(row.get("geneid", ""))
            symbol = (row.get("symbol", "") or "").strip()
            if not geneid:
                continue
            current = entries_by_geneid.get(geneid)
            if current is None:
                entries_by_geneid[geneid] = GeneEntry(geneid=geneid, symbol=symbol)
            elif not current.symbol and symbol:
                entries_by_geneid[geneid] = GeneEntry(geneid=geneid, symbol=symbol)

    return sorted(entries_by_geneid.values(), key=lambda item: item.geneid)


def _string_get(
    session: requests.Session,
    endpoint: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    url = f"{STRING_API_BASE}/{endpoint}"
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Resposta inesperada de {endpoint}: {type(payload)!r}")
    return payload


def _resolve_in_chunks(
    session: requests.Session,
    items: list[tuple[str, str]],
    mode: str,
    call_counter: dict[str, int],
) -> dict[str, tuple[str, str]]:
    resolved: dict[str, tuple[str, str]] = {}
    total_chunks = len(_chunks(items, ID_CHUNK_SIZE))
    for chunk_index, chunk in enumerate(_chunks(items, ID_CHUNK_SIZE), start=1):
        if not chunk:
            continue
        queries = [query for _geneid, query in chunk if query]
        if not queries:
            continue
        query_to_geneids: dict[str, set[str]] = {}
        for geneid, query in chunk:
            query_to_geneids.setdefault(query, set()).add(geneid)

        params = {
            "identifiers": "\r".join(queries),
            "species": STRING_SPECIES_HUMAN,
            "limit": 1,
            "echo_query": 1,
        }
        try:
            payload = _string_get(session, "get_string_ids", params=params)
            call_counter["count"] += 1
        except (HTTPError, Timeout, RequestException, ValueError) as exc:
            LOGGER.warning(
                "Falha ao resolver IDs no STRING (%s chunk %s/%s): %s",
                mode,
                chunk_index,
                total_chunks,
                exc,
            )
            time.sleep(REQUEST_PAUSE_SECONDS)
            continue

        for item in payload:
            query_item = str(item.get("queryItem", "")).strip()
            string_id = str(item.get("stringId", "")).strip()
            preferred_name = str(item.get("preferredName", "")).strip()
            if not query_item or not string_id:
                continue
            for geneid in query_to_geneids.get(query_item, set()):
                if geneid not in resolved:
                    resolved[geneid] = (string_id, preferred_name)
        time.sleep(REQUEST_PAUSE_SECONDS)

    return resolved


def _resolve_genes(
    session: requests.Session,
    genes: list[GeneEntry],
    call_counter: dict[str, int],
) -> tuple[list[ResolvedGene], list[GeneEntry]]:
    items_geneid = [(entry.geneid, entry.geneid) for entry in genes]
    by_geneid = _resolve_in_chunks(session, items_geneid, mode="geneid", call_counter=call_counter)

    unresolved = [entry for entry in genes if entry.geneid not in by_geneid and entry.symbol]
    items_symbol = [(entry.geneid, entry.symbol) for entry in unresolved]
    by_symbol = _resolve_in_chunks(session, items_symbol, mode="symbol", call_counter=call_counter)

    resolved_genes: list[ResolvedGene] = []
    not_mapped: list[GeneEntry] = []
    for entry in genes:
        if entry.geneid in by_geneid:
            string_id, preferred_name = by_geneid[entry.geneid]
            resolved_genes.append(
                ResolvedGene(
                    geneid=entry.geneid,
                    symbol=entry.symbol,
                    string_id=string_id,
                    preferred_name=preferred_name,
                    resolved_by="geneid",
                )
            )
        elif entry.geneid in by_symbol:
            string_id, preferred_name = by_symbol[entry.geneid]
            resolved_genes.append(
                ResolvedGene(
                    geneid=entry.geneid,
                    symbol=entry.symbol,
                    string_id=string_id,
                    preferred_name=preferred_name,
                    resolved_by="symbol",
                )
            )
        else:
            not_mapped.append(entry)

    return resolved_genes, not_mapped


def _build_ppi_rows(
    session: requests.Session,
    resolved_genes: list[ResolvedGene],
    required_score: int,
    call_counter: dict[str, int],
) -> tuple[list[dict[str, Any]], int]:
    source_by_string_id: dict[str, list[ResolvedGene]] = {}
    for gene in resolved_genes:
        source_by_string_id.setdefault(gene.string_id, []).append(gene)

    mapped_string_ids = sorted(source_by_string_id.keys())
    raw_rows: list[dict[str, Any]] = []
    raw_edges = 0
    total_chunks = len(_chunks(mapped_string_ids, NETWORK_CHUNK_SIZE))

    for chunk_index, chunk_ids in enumerate(_chunks(mapped_string_ids, NETWORK_CHUNK_SIZE), start=1):
        if not chunk_ids:
            continue

        params = {
            "identifiers": "\r".join(chunk_ids),
            "species": STRING_SPECIES_HUMAN,
            "required_score": required_score,
        }
        try:
            payload = _string_get(session, "network", params=params)
            call_counter["count"] += 1
        except (HTTPError, Timeout, RequestException, ValueError) as exc:
            LOGGER.warning(
                "Falha ao consultar rede STRING (chunk %s/%s): %s",
                chunk_index,
                total_chunks,
                exc,
            )
            time.sleep(REQUEST_PAUSE_SECONDS)
            continue

        raw_edges += len(payload)
        for edge in payload:
            species = str(edge.get("ncbiTaxonId", "")).strip()
            if species != str(STRING_SPECIES_HUMAN):
                continue

            source_a = str(edge.get("stringId_A", "")).strip()
            source_b = str(edge.get("stringId_B", "")).strip()
            if not source_a or not source_b:
                continue

            pref_a = str(edge.get("preferredName_A", "")).strip()
            pref_b = str(edge.get("preferredName_B", "")).strip()
            escore = _safe_float(edge.get("escore"))
            dscore = _safe_float(edge.get("dscore"))
            tscore = _safe_float(edge.get("tscore"))
            score = _safe_float(edge.get("score"))

            if source_a in source_by_string_id:
                for source_gene in source_by_string_id[source_a]:
                    raw_rows.append(
                        {
                            "geneid": source_gene.geneid,
                            "node2": pref_b or source_b,
                            "node1_string_id": source_a,
                            "node2_string_id": source_b,
                            "experimentally_determined_interaction": escore,
                            "database_annotated": dscore,
                            "automated_textmining": tscore,
                            "combined_score": score,
                        }
                    )
            if source_b in source_by_string_id:
                for source_gene in source_by_string_id[source_b]:
                    raw_rows.append(
                        {
                            "geneid": source_gene.geneid,
                            "node2": pref_a or source_a,
                            "node1_string_id": source_b,
                            "node2_string_id": source_a,
                            "experimentally_determined_interaction": escore,
                            "database_annotated": dscore,
                            "automated_textmining": tscore,
                            "combined_score": score,
                        }
                    )
        time.sleep(REQUEST_PAUSE_SECONDS)

    return raw_rows, raw_edges


def _deduplicate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dedup_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        geneid = str(row["geneid"])
        node_a = str(row["node1_string_id"])
        node_b = str(row["node2_string_id"])
        edge_key = (geneid, min(node_a, node_b), max(node_a, node_b))
        if edge_key not in dedup_map:
            dedup_map[edge_key] = row
            continue
        current_score = _safe_float(dedup_map[edge_key].get("combined_score"))
        candidate_score = _safe_float(row.get("combined_score"))
        if candidate_score > current_score:
            dedup_map[edge_key] = row
    return list(dedup_map.values())


def _write_output(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in OUTPUT_COLUMNS})


def run_gold_edge_ppi(**_context) -> None:
    """Gera ``gold_edge_ppi.csv`` a partir de ``silver_geo_nodes_principal.csv``."""
    silver_path = data_dir() / "silver" / SILVER_GEO_NODES_PRINCIPAL_CSV
    output_path = data_dir() / "gold" / "gold_edge_ppi.csv"
    required_score = DEFAULT_REQUIRED_SCORE
    call_counter = {"count": 0}

    genes = _read_input_genes(silver_path)
    if not genes:
        LOGGER.warning("Nenhum geneid valido encontrado em %s; gerando output vazio.", silver_path)
        _write_output(output_path, [])
        return

    LOGGER.info("PPI Gold iniciado: genes de entrada (unicos por geneid)=%s", len(genes))
    with requests.Session() as session:
        resolved_genes, not_mapped = _resolve_genes(session, genes, call_counter)
        mapped_by_geneid = sum(1 for gene in resolved_genes if gene.resolved_by == "geneid")
        mapped_by_symbol = sum(1 for gene in resolved_genes if gene.resolved_by == "symbol")
        LOGGER.info(
            "Genes mapeados no STRING=%s (geneid=%s, fallback_symbol=%s, nao_mapeados=%s)",
            len(resolved_genes),
            mapped_by_geneid,
            mapped_by_symbol,
            len(not_mapped),
        )
        if not_mapped:
            sample = ", ".join(f"{gene.geneid}:{gene.symbol}" for gene in not_mapped[:10])
            LOGGER.warning("Amostra de genes nao mapeados (%s): %s", min(10, len(not_mapped)), sample)

        raw_rows, raw_edges = _build_ppi_rows(
            session=session,
            resolved_genes=resolved_genes,
            required_score=required_score,
            call_counter=call_counter,
        )

    dedup_rows = _deduplicate_rows(raw_rows)
    dedup_rows.sort(key=lambda row: (str(row["geneid"]), str(row["node1_string_id"]), str(row["node2_string_id"])))
    _write_output(output_path, dedup_rows)

    LOGGER.info("Chamadas HTTP totais para STRING=%s", call_counter["count"])
    LOGGER.info("Arestas brutas retornadas pela API=%s", raw_edges)
    LOGGER.info("Arestas finais apos deduplicacao=%s", len(dedup_rows))
    LOGGER.info("Arquivo Gold PPI salvo em %s", output_path)
