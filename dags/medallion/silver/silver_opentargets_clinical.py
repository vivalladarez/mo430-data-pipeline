"""Camada silver: ingestão e validação de associações clínicas via Open Targets GraphQL."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests import HTTPError, RequestException, Timeout

from medallion.silver.silver import SILVER_GEO_NODES_PRINCIPAL_CSV
from utils.paths import data_dir

LOGGER = logging.getLogger(__name__)

SILVER_OPENTARGETS_CLINICAL_CSV = "silver_opentargets_clinical.csv"

OPENTARGETS_GRAPHQL_URLS: tuple[str, ...] = (
    "https://platform.opentargets.org/api/v4/graphql",
    "https://api.platform.opentargets.org/api/v4/graphql",
)

ASSOCIATED_DISEASES_QUERY = """
query associatedDiseases($targetId: String!, $pageIndex: Int!, $pageSize: Int!) {
  target(ensemblId: $targetId) {
    id
    approvedSymbol
    associatedDiseases(page: { index: $pageIndex, size: $pageSize }) {
      count
      rows {
        disease {
          id
          name
        }
        datasourceScores {
          id
          score
        }
      }
    }
  }
}
"""

SEARCH_TARGET_QUERY = """
query searchTarget($queryString: String!) {
  search(
    queryString: $queryString
    entityNames: ["target"]
    page: { index: 0, size: 5 }
  ) {
    hits {
      id
      name
      object {
        ... on Target {
          id
          approvedSymbol
        }
      }
    }
  }
}
"""

REQUEST_TIMEOUT: tuple[float, float] = (10.0, 90.0)
REQUEST_PAUSE_SECONDS = 0.25
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2.0
DISEASE_PAGE_SIZE = 500
ENSEMBL_ID_PATTERN = re.compile(r"^ENSG\d{11}$", re.IGNORECASE)

BASE_OUTPUT_COLUMNS = [
    "input_geneid",
    "input_symbol",
    "target_id",
    "approved_symbol",
    "disease_id",
    "disease_name",
]


@dataclass(frozen=True)
class GeneInput:
    geneid: str
    symbol: str


def _normalize_geneid(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw or raw.lower() == "nan":
        return ""
    try:
        as_float = float(raw)
        if as_float.is_integer():
            return str(int(as_float))
    except ValueError:
        return raw
    return raw


def _read_input_genes(silver_path: Path) -> list[GeneInput]:
    path = Path(silver_path)
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo silver nao encontrado: {path}")

    frame = pd.read_csv(path)
    if "symbol" not in frame.columns:
        raise ValueError(f"CSV silver sem coluna 'symbol': {path}")

    by_key: dict[tuple[str, str], GeneInput] = {}
    for _, row in frame.iterrows():
        geneid = _normalize_geneid(row.get("geneid", ""))
        symbol = str(row.get("symbol", "") or "").strip()
        if not symbol and not geneid:
            continue
        key = (geneid, symbol.upper() if symbol else "")
        if key not in by_key:
            by_key[key] = GeneInput(geneid=geneid, symbol=symbol)

    return sorted(by_key.values(), key=lambda item: (item.symbol, item.geneid))


def _is_ensembl_id(value: str) -> bool:
    return bool(ENSEMBL_ID_PATTERN.match((value or "").strip()))


def _active_graphql_url(session: requests.Session) -> str:
    probe = {"query": "{ __typename }"}
    for url in OPENTARGETS_GRAPHQL_URLS:
        try:
            response = session.post(
                url,
                json=probe,
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code == 200 and "data" in response.json():
                LOGGER.info("Open Targets GraphQL endpoint ativo: %s", url)
                return url
        except (HTTPError, Timeout, RequestException, ValueError) as exc:
            LOGGER.debug("Endpoint %s indisponivel: %s", url, exc)
    raise RuntimeError(
        "Nenhum endpoint Open Targets GraphQL respondeu com sucesso: "
        + ", ".join(OPENTARGETS_GRAPHQL_URLS)
    )


def _post_graphql(
    session: requests.Session,
    url: str,
    query: str,
    variables: dict[str, Any],
) -> dict[str, Any]:
    payload = {"query": query, "variables": variables}
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code in {429, 502, 503, 504}:
                raise HTTPError(
                    f"HTTP {response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                raise ValueError(f"GraphQL errors: {body['errors']}")
            data = body.get("data")
            if data is None:
                raise ValueError(f"Resposta GraphQL sem 'data': {body!r}")
            return data
        except (HTTPError, Timeout, RequestException, ValueError) as exc:
            last_error = exc
            if attempt >= MAX_RETRIES:
                break
            sleep_for = RETRY_BACKOFF_SECONDS * attempt
            LOGGER.warning(
                "Falha GraphQL (tentativa %s/%s): %s; nova tentativa em %.1fs",
                attempt,
                MAX_RETRIES,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)

    assert last_error is not None
    raise last_error


def _resolve_ensembl_id(
    session: requests.Session,
    url: str,
    gene: GeneInput,
) -> str | None:
    candidates: list[str] = []
    if _is_ensembl_id(gene.geneid):
        candidates.append(gene.geneid.strip())
    if _is_ensembl_id(gene.symbol):
        candidates.append(gene.symbol.strip())
    if candidates:
        return candidates[0]

    if not gene.symbol:
        LOGGER.warning("Gene sem symbol para resolver Ensembl (geneid=%s)", gene.geneid)
        return None

    data = _post_graphql(
        session,
        url,
        SEARCH_TARGET_QUERY,
        {"queryString": gene.symbol},
    )
    hits = (data.get("search") or {}).get("hits") or []
    symbol_upper = gene.symbol.upper()

    for hit in hits:
        target_obj = hit.get("object") or {}
        ensembl_id = str(target_obj.get("id") or hit.get("id") or "").strip()
        approved = str(target_obj.get("approvedSymbol") or hit.get("name") or "").strip()
        if not ensembl_id or not _is_ensembl_id(ensembl_id):
            continue
        if approved.upper() == symbol_upper:
            return ensembl_id

    if hits:
        first = hits[0]
        target_obj = first.get("object") or {}
        ensembl_id = str(target_obj.get("id") or first.get("id") or "").strip()
        if _is_ensembl_id(ensembl_id):
            LOGGER.info(
                "Resolucao aproximada por busca: %s -> %s (primeiro hit)",
                gene.symbol,
                ensembl_id,
            )
            return ensembl_id

    LOGGER.warning(
        "Nao foi possivel mapear para Ensembl: geneid=%s symbol=%s",
        gene.geneid,
        gene.symbol,
    )
    return None


def _fetch_associated_disease_rows(
    session: requests.Session,
    url: str,
    ensembl_id: str,
) -> tuple[str, str, list[dict[str, Any]]]:
    flat_rows: list[dict[str, Any]] = []
    target_id = ensembl_id
    approved_symbol = ""
    page_index = 0
    total_count = 0

    while True:
        data = _post_graphql(
            session,
            url,
            ASSOCIATED_DISEASES_QUERY,
            {
                "targetId": ensembl_id,
                "pageIndex": page_index,
                "pageSize": DISEASE_PAGE_SIZE,
            },
        )
        target = data.get("target")
        if not target:
            break

        target_id = str(target.get("id") or ensembl_id)
        approved_symbol = str(target.get("approvedSymbol") or "")
        assoc = target.get("associatedDiseases") or {}
        total_count = int(assoc.get("count") or 0)
        rows = assoc.get("rows") or []
        if not rows:
            break

        for row in rows:
            disease = row.get("disease") or {}
            score_map: dict[str, float] = {}
            for item in row.get("datasourceScores") or []:
                ds_id = str(item.get("id") or "").strip()
                if not ds_id:
                    continue
                try:
                    score_map[ds_id] = float(item.get("score"))
                except (TypeError, ValueError):
                    continue
            flat_rows.append(
                {
                    "disease_id": str(disease.get("id") or ""),
                    "disease_name": str(disease.get("name") or ""),
                    "_scores": score_map,
                }
            )

        if len(flat_rows) >= total_count or len(rows) < DISEASE_PAGE_SIZE:
            break
        page_index += 1
        time.sleep(REQUEST_PAUSE_SECONDS)

    return target_id, approved_symbol, flat_rows


def _score_column_name(datasource_id: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", datasource_id.strip().lower())
    safe = safe.strip("_")
    return f"score_{safe}" if safe else "score_unknown"


def _build_dataframe(
    gene_rows: list[dict[str, Any]],
    datasource_ids: set[str],
) -> pd.DataFrame:
    score_columns = sorted(_score_column_name(ds_id) for ds_id in datasource_ids)
    columns = [*BASE_OUTPUT_COLUMNS, *score_columns]

    if not gene_rows:
        return pd.DataFrame(columns=columns)

    normalized: list[dict[str, Any]] = []
    for row in gene_rows:
        out = {column: row.get(column, "") for column in BASE_OUTPUT_COLUMNS}
        scores: dict[str, float] = row.get("_scores") or {}
        for ds_id, value in scores.items():
            out[_score_column_name(ds_id)] = value
        normalized.append(out)

    frame = pd.DataFrame(normalized)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns]


def correlate_opentargets_with_geo_nodes(
    opentargets: pd.DataFrame,
    geo_nodes: pd.DataFrame,
    *,
    ot_join_col: str | None = "input_geneid",
) -> pd.DataFrame:
    """Inner join Open Targets com tabela de genes (principal silver ou gold GEO).

    Se ``ot_join_col`` for informado e diferente de ``geneid``, cruza por essa coluna
    (ingestão API). Caso contrário, usa ``geneid`` e mantém colunas do ``geo_nodes`` como base.
    """
    if geo_nodes.empty:
        return opentargets.iloc[0:0].copy()

    ot = opentargets.copy()
    geo = geo_nodes.copy()

    if "geneid" not in geo.columns:
        raise ValueError("Tabela de genes precisa da coluna 'geneid'")

    geo["geneid"] = pd.to_numeric(geo["geneid"], errors="coerce").astype("Int64")

    if ot_join_col and ot_join_col in ot.columns and ot_join_col != "geneid":
        if ot.empty:
            return ot
        ot["_join_geneid"] = pd.to_numeric(ot[ot_join_col], errors="coerce").astype("Int64")
        geo_only_cols = [
            column
            for column in geo.columns
            if column not in ot.columns and column != "geneid"
        ]
        geo_add = geo[["geneid", *geo_only_cols]]
        merged = ot.merge(geo_add, left_on="_join_geneid", right_on="geneid", how="inner")
        return merged.drop(columns=["_join_geneid"], errors="ignore")

    if ot.empty:
        return ot
    if "geneid" not in ot.columns:
        raise ValueError("Open Targets correlacionado precisa da coluna 'geneid'")

    ot["geneid"] = pd.to_numeric(ot["geneid"], errors="coerce").astype("Int64")
    ot_only_cols = [
        column for column in ot.columns if column not in geo.columns and column != "geneid"
    ]
    return geo.merge(ot[["geneid", *ot_only_cols]], on="geneid", how="inner")


def run_silver_opentargets_clinical(**_context) -> None:
    """Ingestão Open Targets correlacionada com ``silver_geo_nodes_principal``."""
    principal_path = data_dir() / "silver" / SILVER_GEO_NODES_PRINCIPAL_CSV
    output_path = data_dir() / "silver" / SILVER_OPENTARGETS_CLINICAL_CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)

    genes = _read_input_genes(principal_path)
    if not genes:
        LOGGER.warning("Nenhum gene valido em %s; gravando silver vazio.", principal_path)
        _build_dataframe([], set()).to_csv(output_path, index=False)
        return

    LOGGER.info(
        "Open Targets Silver iniciado: genes unicos=%s origem=%s",
        len(genes),
        principal_path,
    )

    all_rows: list[dict[str, Any]] = []
    datasource_ids: set[str] = set()
    failed: list[GeneInput] = []

    with requests.Session() as session:
        graphql_url = _active_graphql_url(session)

        for index, gene in enumerate(genes, start=1):
            LOGGER.info(
                "Processando gene %s/%s: symbol=%s geneid=%s",
                index,
                len(genes),
                gene.symbol,
                gene.geneid,
            )
            try:
                ensembl_id = _resolve_ensembl_id(session, graphql_url, gene)
                if not ensembl_id:
                    failed.append(gene)
                    time.sleep(REQUEST_PAUSE_SECONDS)
                    continue

                target_id, approved_symbol, disease_rows = _fetch_associated_disease_rows(
                    session,
                    graphql_url,
                    ensembl_id,
                )
                LOGGER.info(
                    "Gene %s (%s): associacoes=%s",
                    gene.symbol,
                    ensembl_id,
                    len(disease_rows),
                )

                for disease_row in disease_rows:
                    scores = disease_row.pop("_scores", {})
                    datasource_ids.update(scores.keys())
                    all_rows.append(
                        {
                            "input_geneid": gene.geneid,
                            "input_symbol": gene.symbol,
                            "target_id": target_id,
                            "approved_symbol": approved_symbol,
                            "disease_id": disease_row["disease_id"],
                            "disease_name": disease_row["disease_name"],
                            "_scores": scores,
                        }
                    )
            except (HTTPError, Timeout, RequestException, ValueError, RuntimeError) as exc:
                LOGGER.exception(
                    "Falha ao processar gene symbol=%s geneid=%s: %s",
                    gene.symbol,
                    gene.geneid,
                    exc,
                )
                failed.append(gene)

            time.sleep(REQUEST_PAUSE_SECONDS)

    api_result = _build_dataframe(all_rows, datasource_ids)
    principal = pd.read_csv(principal_path)
    correlated = correlate_opentargets_with_geo_nodes(
        api_result,
        principal,
        ot_join_col="input_geneid",
    )
    correlated.to_csv(output_path, index=False)

    LOGGER.info("Linhas API Open Targets=%s", len(api_result))
    LOGGER.info("Linhas silver correlacionadas (principal)=%s", len(correlated))
    LOGGER.info("Genes com falha ou sem mapeamento=%s", len(failed))
    LOGGER.info("Arquivo silver salvo em %s", output_path)
