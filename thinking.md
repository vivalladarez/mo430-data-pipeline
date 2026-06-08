# Open Targets Gold — thinking & debug log

**Date:** 2026-06-04  
**Task:** Enrich `silver_geo_nodes_principal.csv` with clinical disease associations via Open Targets GraphQL.

---

## 1. Context & architecture

| Layer | File | Role |
|-------|------|------|
| Silver (input genes) | `data/silver/silver_geo_nodes_principal.csv` | Curated genes: NCBI `geneid`, HGNC `symbol`, expression metadata |
| Silver (Open Targets) | `data/silver/silver_opentargets_clinical.csv` | API Open Targets **+** correlação com `silver_geo_nodes_principal` |
| Gold (output) | `data/gold/gold_opentargets_clinical.csv` | Silver OT **+** correlação com `gold_geo_nodes` (GEO final GSE×NOS) |

**Refactor (2026-06-04):** silver = ingestão + join principal; gold = join `gold_geo_nodes` (não principal direto).

**Design choices (aligned with `gold_edge_ppi.py`):**

- Paths via `utils.paths.data_dir()` and `AIRFLOW_HOME` (project root when running locally).
- `requests.Session`, explicit timeouts `(10, 90)s`, pause `0.25s` between genes, retry loop (4 attempts, linear backoff).
- Pandas for read/flatten/write; standard `logging` for Airflow log capture.
- Silver: `run_silver_opentargets_clinical` → API + `correlate_opentargets_with_geo_nodes(..., principal)`.
- Gold: `run_gold_opentargets_clinical` → `correlate_opentargets_with_geo_nodes(..., gold_geo_nodes)`.
- DAG: `principal >> silver_opentargets`; `[gold_geo_nodes, silver_opentargets] >> gold_opentargets`.

---

## 2. Input data inspection

`silver_geo_nodes_principal.csv` has **278 lines** (277 genes + header).

- Columns: `symbol`, `description`, `geneid`, `log2foldchange`, …
- `geneid` values are **NCBI Entrez IDs** (e.g. `54206`), not Ensembl.
- Open Targets `target(ensemblId: …)` requires **Ensembl IDs** (`ENSG…`).

**Implication:** Must resolve symbol → Ensembl before the user-supplied `associatedDiseases` query.

---

## 3. API endpoint troubleshooting

### 3.1 User-specified URL → HTTP 405

```text
POST https://platform.opentargets.org/api/v4/graphql
→ 405 Not Allowed (nginx)
```

Tested with `curl` and `requests.post`; same result.

### 3.2 Documented API host → HTTP 200

Per [Open Targets GraphQL docs](https://platform-docs.opentargets.org/data-access/graphql-api):

```text
POST https://api.platform.opentargets.org/api/v4/graphql
→ 200, valid JSON
```

**Implementation:** `OPENTARGETS_GRAPHQL_URLS` tries the spec URL first, then falls back to `api.platform.opentargets.org`. At runtime, probe `{ __typename }` selects the first working endpoint. Airflow logs showed:

```text
Open Targets GraphQL endpoint ativo: https://api.platform.opentargets.org/api/v4/graphql
```

---

## 4. GraphQL queries

### 4.1 Target resolution (search)

NCBI `geneid` alone does **not** return hits in Open Targets search (`queryString: "54206"` → empty). **Symbol search works** (`ERRFI1` → `ENSG00000116285`).

Search query uses `entityNames: ["target"]` and prefers an exact `approvedSymbol` match; otherwise first hit with a warning.

### 4.2 Associated diseases (user template + pagination)

User template extended with pagination variables (required for targets like TP53 / VEGFA with thousands of associations):

```graphql
associatedDiseases(page: { index: $pageIndex, size: $pageSize })
```

- Page size: **500** (verified: ERRFI1 returns all 232 rows in one page).
- Loop until `len(accumulated) >= count` or partial page.

### 4.3 Score flattening

Each association row has `datasourceScores: [{ id, score }, …]`.

- Mapped to columns `score_<datasource_id>` (normalized: lowercase, non-alphanumerics → `_`).
- Examples in output: `score_gwas_credible_sets`, `score_europepmc`, `score_crispr_screen`, `score_crispr`, …
- Union of all datasource IDs seen across genes defines the final column set (**20 score columns** in this run).

---

## 5. Output schema

| Column | Description |
|--------|-------------|
| `input_geneid` | Entrez ID from silver |
| `input_symbol` | Symbol from silver |
| `target_id` | Ensembl ID from Open Targets |
| `approved_symbol` | Open Targets approved symbol |
| `disease_id` | EFO/MONDO/etc. |
| `disease_name` | Human-readable disease name |
| `score_*` | One column per datasource (sparse NaN where absent) |

**Run stats:**

- Genes processed: **278** unique (geneid, symbol) pairs
- Output rows: **76,490**
- Unmapped genes: **4** (no Ensembl hit)

---

## 6. Genes without Ensembl mapping

| symbol | geneid | Notes |
|--------|--------|-------|
| HEAT2 | 113633881 | No search hits |
| IGHV3-9 | 28451 | Immunoglobulin variable region; often absent from OT target index |
| LOC102723407 | 102723407 | LOC placeholder symbol |
| TAMALIN-AS1 | 692159 | Antisense / lncRNA style symbol |

These are logged as `WARNING` and skipped; the pipeline does not fail.

---

## 7. Data protection validation

Before/after Airflow `tasks test`, **unchanged** (mtime Jun 4 14:36–14:38):

- `data/silver/silver_geo_nodes_principal.csv`
- `data/gold/gold_geo_nodes.csv`
- `data/gold/gold_edge_ppi.csv`

Only **new/overwritten** artifact: `data/gold/gold_opentargets_clinical.csv` (expected).

Bronze files were not read or written by this task.

---

## 8. Execution & Airflow validation

### 8.1 Direct callable (dev)

```bash
export AIRFLOW_HOME=/Users/carlosaraki/Documents/mo430-data-pipeline
export PYTHONPATH=$AIRFLOW_HOME/dags
python3 -c "from medallion.gold.gold_opentargets_clinical import run_gold_opentargets_clinical; run_gold_opentargets_clinical()"
```

- Duration: ~4.4 minutes (278 genes, ~2 HTTP calls per gene + pagination for heavy targets).
- Result: 76,490 rows written.

### 8.2 Airflow

```bash
pip install "apache-airflow>=2.8.0,<3.0.0"
airflow db migrate
airflow tasks test medallion_sample_pipeline gold_opentargets_clinical 2025-01-01
```

- Task status: **SUCCESS**
- Log file: `/tmp/ot_airflow_test.log` (local capture); Airflow also writes under `$AIRFLOW_HOME/logs/…`
- Scheduler/webserver were **not** required for `tasks test`.

### 8.3 DAG wiring

File: `dags/medallion_pipeline_dag.py`

- Import: `run_gold_opentargets_clinical`
- Task id: `gold_opentargets_clinical`
- Dependency: `silver_geo_nodes_principal >> gold_opentargets_clinical`

---

## 9. Resilience summary

| Mechanism | Setting |
|-----------|---------|
| HTTP timeout | `(10, 90)` seconds |
| Retries | 4, backoff `2s × attempt` |
| Rate limit | `0.25s` sleep after each gene |
| Per-gene errors | Logged; gene added to `failed`; continue |
| Empty input | Empty CSV with header columns |
| Endpoint failover | Two URLs, probe on startup |

`tenacity` was not added (not in `requirements.txt`); retries are implemented inline like other project modules.

---

## 10. Files created/modified

| File | Action |
|------|--------|
| `dags/medallion/silver/silver_opentargets_clinical.py` | **Created** (ingestão API) |
| `dags/medallion/gold/gold_opentargets_clinical.py` | **Refactored** (só correlação) |
| `dags/medallion_pipeline_dag.py` | **Updated** (silver + gold tasks) |
| `data/silver/silver_opentargets_clinical.csv` | **Generated** (validação OT) |
| `data/gold/gold_opentargets_clinical.csv` | **Generated** (OT + metadata principal) |
| `thinking.md` | **Created** (this document) |

---

## 11. Follow-ups (optional)

- Map failed symbols via external ID conversion (NCBI → Ensembl) if OT search is insufficient.
- Cap `associatedDiseases` rows per gene (e.g. top N by max score) if 76k+ rows per run is too large for downstream Neo4j loads.
- Pin primary URL to `api.platform.opentargets.org` only if the UI host will not be fixed.
