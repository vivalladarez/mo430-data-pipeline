"""Camada bronze EBI: ingestao de expressao genica publica do COVID-19 Data Portal."""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from requests import Response
from requests.exceptions import HTTPError, RequestException, Timeout

from utils.paths import data_dir

logger = logging.getLogger(__name__)

EBI_SEARCH_URL = "https://www.ebi.ac.uk/ebisearch/ws/rest/atlas-experiments"
GXA_EXPERIMENT_URL_TEMPLATE = "https://www.ebi.ac.uk/gxa/json/experiments/{accession}"
GXA_BASE_URL = "https://www.ebi.ac.uk/gxa/"

JSON_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "mo430-data-pipeline/bronze-ebi",
}

TEXT_HEADERS = {
    "Accept": "text/plain",
    "User-Agent": "mo430-data-pipeline/bronze-ebi",
}


def _request_json(
    session: requests.Session, url: str, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Executa request JSON com timeout e tratamento padrao de erros."""
    try:
        response: Response = session.get(
            url, params=params, headers=JSON_HEADERS, timeout=(10, 60)
        )
        response.raise_for_status()
    except Timeout as exc:
        raise Timeout(f"Timeout ao consultar API EBI em {url}") from exc
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "desconhecido"
        raise HTTPError(f"Erro HTTP {status} ao consultar API EBI em {url}") from exc
    except RequestException as exc:
        raise RequestException(f"Falha de rede ao consultar API EBI em {url}") from exc

    return response.json()


def _request_text(session: requests.Session, url: str) -> str:
    """Baixa conteudo tabular (TSV) de experimento Expression Atlas."""
    try:
        response: Response = session.get(url, headers=TEXT_HEADERS, timeout=(10, 90))
        response.raise_for_status()
    except Timeout as exc:
        raise Timeout(f"Timeout ao baixar matriz de expressao em {url}") from exc
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "desconhecido"
        raise HTTPError(f"Erro HTTP {status} ao baixar matriz de expressao em {url}") from exc
    except RequestException as exc:
        raise RequestException(f"Falha de rede ao baixar matriz de expressao em {url}") from exc

    return response.text


def _discover_experiments(
    session: requests.Session, query: str, page_size: int, max_experiments: int
) -> list[str]:
    """Descobre acessos de experimentos via endpoint paginado do EBI Search."""
    accessions: list[str] = []
    start = 0
    page = 1
    total_hits: int | None = None

    while len(accessions) < max_experiments:
        if total_hits is not None and start >= total_hits:
            logger.info("Fim da paginacao: start=%d >= total_hits=%d.", start, total_hits)
            break

        params = {
            "query": query,
            "format": "json",
            "size": page_size,
            "start": start,
        }
        logger.info("Consultando pagina %d da busca EBI: %s", page, params)
        payload = _request_json(session, EBI_SEARCH_URL, params=params)
        if total_hits is None:
            raw_hit_count = payload.get("hitCount")
            if isinstance(raw_hit_count, int):
                total_hits = raw_hit_count
                logger.info("Total de hits reportados pela API: %d", total_hits)

        entries = payload.get("entries", [])
        if not entries:
            logger.info("Nenhuma entrada adicional encontrada na pagina %d.", page)
            break

        for entry in entries:
            accession = entry.get("id")
            if isinstance(accession, str) and accession:
                accessions.append(accession)
                if len(accessions) >= max_experiments:
                    break

        start += page_size
        page += 1

    return accessions


def _parse_expression_rows(
    tsv_payload: str,
    *,
    accession: str,
    experiment_type: str,
    species: str,
    source_url: str,
    ingested_at: str,
    max_rows_per_experiment: int,
) -> list[dict[str, str]]:
    """Transforma TSV de Expression Atlas em linhas normalizadas para CSV bronze."""
    clean_lines = [line for line in tsv_payload.splitlines() if line and not line.startswith("#")]
    if not clean_lines:
        return []

    reader = csv.reader(io.StringIO("\n".join(clean_lines)), delimiter="\t")
    header = next(reader, [])
    if len(header) < 3:
        logger.warning("Experimento %s sem colunas de expressao detectadas.", accession)
        return []

    comparison_headers = header[2:]
    output_rows: list[dict[str, str]] = []

    for row in reader:
        if len(row) < 2:
            continue
        gene_id = row[0]
        gene_name = row[1]
        values = row[2:]
        for comparison_label, expression_value in zip(comparison_headers, values):
            output_rows.append(
                {
                    "experiment_accession": accession,
                    "experiment_type": experiment_type,
                    "species": species,
                    "gene_id": gene_id,
                    "gene_name": gene_name,
                    "comparison_label": comparison_label,
                    "expression_value": expression_value,
                    "source_url": source_url,
                    "ingested_at": ingested_at,
                }
            )
            if len(output_rows) >= max_rows_per_experiment:
                return output_rows

    return output_rows


def run_bronze_ebi(**context: Any) -> None:
    """Ingere dados publicos de expressao genica do EBI para a camada bronze."""
    del context  # kwargs do Airflow nao utilizados nesta etapa.

    query = os.environ.get("EBI_EXPRESSION_QUERY", "sars-cov-2")
    page_size = int(os.environ.get("EBI_PAGE_SIZE", "25"))
    max_experiments = int(os.environ.get("EBI_MAX_EXPERIMENTS", "20"))
    max_rows_per_experiment = int(os.environ.get("EBI_MAX_ROWS_PER_EXPERIMENT", "5000"))
    target_species = os.environ.get("EBI_TARGET_SPECIES", "Homo sapiens").strip()

    out_dir = data_dir() / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "bronze_ebi_expression.csv"
    ingested_at = datetime.now(timezone.utc).isoformat()

    logger.info("Iniciando ingestao EBI de expressao genica")
    logger.info(
        "Busca=%s | page_size=%d | max_experiments=%d | max_rows_per_experiment=%d | target_species=%s",
        query,
        page_size,
        max_experiments,
        max_rows_per_experiment,
        target_species,
    )

    all_rows: list[dict[str, str]] = []
    processed_experiments = 0
    skipped_by_species = 0

    with requests.Session() as session:
        accessions = _discover_experiments(session, query, page_size, max_experiments)
        logger.info("Experimentos descobertos: %d", len(accessions))

        for accession in accessions:
            meta_url = GXA_EXPERIMENT_URL_TEMPLATE.format(accession=accession)
            logger.info("Consultando metadados do experimento %s", accession)
            experiment_payload = _request_json(session, meta_url)
            experiment = experiment_payload.get("experiment", {})

            experiment_type = str(experiment.get("type", "unknown"))
            species = str(experiment.get("species", "unknown"))
            if species.casefold() != target_species.casefold():
                skipped_by_species += 1
                logger.info(
                    "Experimento %s ignorado por especie (%s != %s).",
                    accession,
                    species,
                    target_species,
                )
                continue

            download_path = str(experiment.get("urls", {}).get("download", ""))
            if not download_path:
                logger.warning("Experimento %s sem URL de download; ignorando.", accession)
                continue

            download_url = urljoin(GXA_BASE_URL, download_path)
            logger.info("Baixando matriz de expressao do experimento %s: %s", accession, download_url)
            tsv_payload = _request_text(session, download_url)

            experiment_rows = _parse_expression_rows(
                tsv_payload,
                accession=accession,
                experiment_type=experiment_type,
                species=species,
                source_url=download_url,
                ingested_at=ingested_at,
                max_rows_per_experiment=max_rows_per_experiment,
            )
            all_rows.extend(experiment_rows)
            processed_experiments += 1
            logger.info(
                "Experimento %s processado com %d linhas extraidas.",
                accession,
                len(experiment_rows),
            )

    fieldnames = [
        "experiment_accession",
        "experiment_type",
        "species",
        "gene_id",
        "gene_name",
        "comparison_label",
        "expression_value",
        "source_url",
        "ingested_at",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    logger.info(
        "Ingestao EBI concluida. Experimentos processados: %d | Ignorados por especie: %d | Linhas extraidas: %d | Arquivo: %s",
        processed_experiments,
        skipped_by_species,
        len(all_rows),
        out_path,
    )
