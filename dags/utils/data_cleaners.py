from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

MISSING_TOKENS = {"", "none", "null", "nan", "na", "n/a", "not available"}
REQUIRED_COLUMNS = ("sample_id", "geo_accession", "series_id", "source_name_ch1")
CHARACTERISTICS_COLUMN = "characteristics_ch1"
SILVER_CORE_COLUMNS = (
    "sample_id",
    "geo_accession",
    "series_id",
    "title",
    "source_name_ch1",
    "organism_ch1",
    "molecule_ch1",
    "characteristics_ch1",
    "description",
    "data_processing",
    "platform_id",
    "instrument_model",
    "library_source",
    "library_strategy",
    "relation",
    "ingested_at",
    "is_blood_sample",
)
SILVER_CHAR_COLUMNS = (
    "char_clinical_status",
    "char_age",
    "char_gender",
    "char_ethnicity",
    "char_tissue",
    "char_cell_type",
    "char_timepoint",
    "char_time_of_blood_draw",
    "char_vaccine_product",
)

EBI_REQUIRED_COLUMNS = (
    "experiment_accession",
    "gene_id",
    "gene_name",
    "comparison_label",
    "expression_value",
)
EBI_SILVER_COLUMNS = (
    "experiment_accession",
    "experiment_type",
    "species",
    "gene_id",
    "gene_name",
    "comparison_label",
    "expression_value",
    "expression_value_numeric",
    "source_url",
    "ingested_at",
)

GEO_GENE_REQUIRED_COLUMNS = ("ingested_at",)
GEO_GENE_SILVER_COLUMNS = (
    "series_id",
    "symbol",
    "geneid",
    "gb_acc",
    "description",
    "pvalue",
    "padj",
    "log2foldchange",
    "stat",
    "basemean",
    "ingested_at",
    "bronze_source_file",
)


def _normalize_token(value: object) -> object:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped.lower() in MISSING_TOKENS:
        return pd.NA
    return stripped


def _normalize_key(raw_key: str) -> str:
    key = raw_key.strip().lower()
    key = re.sub(r"[\s/\-]+", "_", key)
    key = re.sub(r"[^a-z0-9_]", "", key)
    return key


def _parse_characteristics(value: object) -> dict[str, str]:
    if not isinstance(value, str) or not value.strip():
        return {}

    parsed: dict[str, str] = {}
    chunks = [part.strip() for part in value.split("||") if part.strip()]
    for chunk in chunks:
        if ":" not in chunk:
            continue
        key, val = chunk.split(":", 1)
        clean_key = _normalize_key(key)
        clean_val = val.strip()
        if clean_key and clean_val:
            parsed[clean_key] = clean_val
    return parsed


def _apply_characteristics_columns(df: pd.DataFrame) -> pd.DataFrame:
    if CHARACTERISTICS_COLUMN not in df.columns:
        return df

    parsed_series = df[CHARACTERISTICS_COLUMN].apply(_parse_characteristics)
    all_keys = sorted(set().union(*(item.keys() for item in parsed_series if item)))
    if not all_keys:
        return df

    for key in all_keys:
        df[f"char_{key}"] = parsed_series.apply(lambda item: item.get(key))

    # Harmoniza sinônimos de status clínico em um único campo
    status_candidates = [
        "char_pasc_status",
        "char_current_long_covid_status",
        "char_long_covid_status",
    ]
    available = [col for col in status_candidates if col in df.columns]
    if available:
        df["char_clinical_status"] = df[available].bfill(axis=1).iloc[:, 0]

    return df


def _drop_rows_missing_required(df: pd.DataFrame, required_columns: Iterable[str]) -> pd.DataFrame:
    present_required = [col for col in required_columns if col in df.columns]
    if not present_required:
        return df
    return df.dropna(subset=present_required, how="any")


def clean_ebi_expression_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Limpeza da bronze Expression Atlas (EBI) para a camada silver."""
    df = df.copy()
    df = df.apply(lambda column: column.map(_normalize_token))
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df = _drop_rows_missing_required(df, EBI_REQUIRED_COLUMNS)
    if df.empty:
        return df.reset_index(drop=True)
    df = df.drop_duplicates(
        subset=["experiment_accession", "gene_id", "comparison_label"], keep="first"
    )
    if "expression_value" in df.columns:
        df["expression_value_numeric"] = pd.to_numeric(
            df["expression_value"], errors="coerce"
        )
    keep = [col for col in EBI_SILVER_COLUMNS if col in df.columns]
    return df[keep].reset_index(drop=True)


def clean_geo_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Limpeza inicial da camada bronze para preparar a camada silver."""
    df = df.copy()

    # Padroniza valores nulos em todas as colunas textuais
    df = df.apply(lambda column: column.map(_normalize_token))

    # Remove linhas ou colunas totalmente vazias
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    # Remove duplicatas (registros com mesmo sample_id)
    if "sample_id" in df.columns:
        df = df.drop_duplicates(subset=["sample_id"])
    else:
        df = df.drop_duplicates()

    # Extrai metadados de characteristics para colunas auxiliares
    df = _apply_characteristics_columns(df)

    # Marca amostras de sangue para facilitar filtragens na análise.
    source = df.get("source_name_ch1", pd.Series(index=df.index, dtype="object")).fillna("")
    char_cell_type = df.get("char_cell_type", pd.Series(index=df.index, dtype="object")).fillna("")
    char_tissue = df.get("char_tissue", pd.Series(index=df.index, dtype="object")).fillna("")
    blood_regex = r"blood|pbmc|plasma|serum"
    df["is_blood_sample"] = (
        source.str.contains(blood_regex, case=False, regex=True)
        | char_cell_type.str.contains(blood_regex, case=False, regex=True)
        | char_tissue.str.contains(blood_regex, case=False, regex=True)
    )

    # Remove registros sem metadados mínimos para rastreabilidade
    df = _drop_rows_missing_required(df, REQUIRED_COLUMNS)

    # Mantém apenas colunas úteis para análise biológica e clínica.
    keep_columns = [
        col
        for col in (*SILVER_CORE_COLUMNS, *SILVER_CHAR_COLUMNS)
        if col in df.columns
    ]
    df = df[keep_columns]

    return df.reset_index(drop=True)


def clean_geo_gene_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Limpeza/padronização de GEO em formato tabular por gene (CSV bruto).

    Aceita variações comuns de colunas (ex.: DESeq2 / limma) e normaliza para um
    schema único, para consolidação em `silver_geo_nodes.csv`.
    """
    df = df.copy()
    df = df.apply(lambda column: column.map(_normalize_token))
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df = _drop_rows_missing_required(df, GEO_GENE_REQUIRED_COLUMNS)
    if df.empty:
        return df.reset_index(drop=True)

    # Mapeia nomes alternativos → nomes canônicos (minúsculos) para padronização.
    rename_map = {
        "ID": "symbol",
        "Symbol": "symbol",
        "GeneID": "geneid",
        "GB_ACC": "gb_acc",
        "Description": "description",
        "pvalue": "pvalue",
        "P.Value": "pvalue",
        "padj": "padj",
        "adj.P.Val": "padj",
        "log2FoldChange": "log2foldchange",
        "logFC": "log2foldchange",
        "stat": "stat",
        "t": "stat",
        "baseMean": "basemean",
        "B": "basemean",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Tipos numéricos quando aplicável
    for col in ("pvalue", "padj", "log2foldchange", "stat", "basemean"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = [col for col in GEO_GENE_SILVER_COLUMNS if col in df.columns]
    return df[keep].reset_index(drop=True)


GEO_NOS_SILVER_COLUMNS = (
    "symbol",
    "description",
    "geneid",
    "log2foldchange",
    "neg_log10_pvalue",
    "ingested_at",
    "bronze_source_file",
    "dataset_id",
)


def clean_geo_nos_nodes_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Silver para tabelas NOS (ex.: NOS0001): colunas minúsculas e filtros básicos.

    Remove linhas em que ``description`` contém *uncharacterized* ou *tRNA*
    (case-insensitive).
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    odd_to_std: dict[str, str] = {}
    for col in df.columns:
        c = col.replace(" ", "")
        if c in ("log2(foldchange)", "log2(fold_change)"):
            odd_to_std[col] = "log2foldchange"
        elif col.replace(" ", "").lower() in ("-log10(pvalue)", "-log10(p)"):
            odd_to_std[col] = "neg_log10_pvalue"
    if odd_to_std:
        df = df.rename(columns=odd_to_std)

    df = df.apply(lambda column: column.map(_normalize_token))
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    if "description" in df.columns:
        desc = df["description"].fillna("").astype(str)
        drop_mask = desc.str.contains(r"uncharacterized|trna", case=False, regex=True)
        df = df.loc[~drop_mask].copy()

    for col in ("geneid", "log2foldchange", "neg_log10_pvalue"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = [col for col in GEO_NOS_SILVER_COLUMNS if col in df.columns]
    return df[keep].reset_index(drop=True)
