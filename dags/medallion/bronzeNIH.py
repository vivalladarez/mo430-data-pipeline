"""Camada bronze NIH: ingestão de extração local PIC-SURE para CSV bronze."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import logging
from typing import Any

from medallion.paths import data_dir

logger = logging.getLogger(__name__)


def run_bronze_nih(**context: Any) -> None:
    """Ingere NIH.csv na camada bronze e adiciona metadado de rastreabilidade."""
    del context  # kwargs do Airflow não usados nesta etapa.

    raw_path = data_dir() / "raw" / "NIH.csv"
    out_dir = data_dir() / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "bronze_NIH.csv"

    logger.info("Iniciando ingestao NIH da camada bronze")
    logger.info("Arquivo de entrada: %s", raw_path)
    logger.info("Arquivo de saida: %s", out_path)

    if not raw_path.is_file():
        raise FileNotFoundError(
            f"Arquivo bruto NIH nao encontrado em '{raw_path}'. "
            "Crie o arquivo data/raw/NIH.csv antes de executar a DAG."
        )

    ingested_at = datetime.now(timezone.utc).isoformat()
    row_count = 0
    with raw_path.open(newline="", encoding="utf-8") as src, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        reader = csv.DictReader(src)
        if reader.fieldnames is None:
            logger.warning(
                "CSV NIH sem cabecalho detectado; apenas a coluna ingested_at sera garantida."
            )
            fieldnames = ["ingested_at"]
        else:
            fieldnames = [*reader.fieldnames, "ingested_at"]

        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            row["ingested_at"] = ingested_at
            writer.writerow(row)
            row_count += 1

    logger.info("Ingestao NIH finalizada com sucesso. Linhas processadas: %d", row_count)


# Futuro: migracao de ingestao estatica (CSV local) para API PicSureHpdsLib
# 1) Autenticacao e bootstrap do cliente:
#    - Ler token e endpoint de variaveis de ambiente.
#    - Inicializar o cliente PicSureHpdsLib com timeout e retries.
# 2) Execucao de consulta no NIH Biodata Catalyst (PIC-SURE):
#    - Montar query com cohort/filtros clinicos e de bioinformatica.
#    - Paginar respostas quando necessario para evitar estouro de memoria.
# 3) Serializacao para formato tabular bronze:
#    - Normalizar payload JSON para colunas CSV estaveis.
#    - Preservar tipagem basica e valores ausentes de forma explicita.
# 4) Rastreabilidade e versionamento:
#    - Manter coluna ingested_at em UTC.
#    - Incluir metadados de query/versionamento para reprodutibilidade.
#
# Variaveis de ambiente sugeridas para esta evolucao:
# - PIC_SURE_TOKEN
# - PIC_SURE_BASE_URL
# - PIC_SURE_RESOURCE_UUID
# - PIC_SURE_PROJECT_ID
