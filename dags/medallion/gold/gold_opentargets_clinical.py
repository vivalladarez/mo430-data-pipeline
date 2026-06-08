"""Camada gold: correlação Open Targets (silver) com ``gold_geo_nodes`` final."""

from __future__ import annotations

import logging

import pandas as pd

from medallion.gold.gold import GOLD_GEO_NODES_CSV
from medallion.silver.silver_opentargets_clinical import (
    SILVER_OPENTARGETS_CLINICAL_CSV,
    correlate_opentargets_with_geo_nodes,
)
from utils.paths import data_dir

LOGGER = logging.getLogger(__name__)

GOLD_OPENTARGETS_CLINICAL_CSV = "gold_opentargets_clinical.csv"


def run_gold_opentargets_clinical(**_context) -> None:
    """Cruza ``silver_opentargets_clinical`` com ``gold_geo_nodes`` por ``geneid``.

    Mantém apenas genes presentes no gold GEO final (inner join). Expressão/metadata
    vêm de ``gold_geo_nodes``; scores e doenças Open Targets entram como colunas extras.
    Grava ``data/gold/gold_opentargets_clinical.csv``.
    """
    silver_dir = data_dir() / "silver"
    gold_dir = data_dir() / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    ot_path = silver_dir / SILVER_OPENTARGETS_CLINICAL_CSV
    geo_path = gold_dir / GOLD_GEO_NODES_CSV
    out_path = gold_dir / GOLD_OPENTARGETS_CLINICAL_CSV

    if not ot_path.is_file():
        raise FileNotFoundError(
            f"Silver Open Targets nao encontrado: {ot_path} "
            "(execute silver_opentargets_clinical antes)"
        )
    if not geo_path.is_file():
        raise FileNotFoundError(
            f"Gold GEO nao encontrado: {geo_path} (execute gold_geo_nodes antes)"
        )

    ot = pd.read_csv(ot_path)
    geo = pd.read_csv(geo_path)

    if ot.empty:
        LOGGER.warning("Silver Open Targets vazio; gravando gold vazio.")
        ot.to_csv(out_path, index=False)
        return

    if "geneid" not in ot.columns:
        raise ValueError(
            f"Silver Open Targets sem 'geneid': {ot_path} "
            "(reexecute silver_opentargets_clinical para correlacionar com principal)"
        )

    merged = correlate_opentargets_with_geo_nodes(ot, geo, ot_join_col=None)
    merged.to_csv(out_path, index=False)

    LOGGER.info(
        "Gold Open Targets: linhas silver=%s, linhas gold (apos gold_geo_nodes)=%s",
        len(ot),
        len(merged),
    )
    LOGGER.info("Arquivo gold salvo em %s", out_path)
