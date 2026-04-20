# mo430-data-pipeline

Orquestração de pipeline de dados com **Apache Airflow** (instalação via **pip**, sem Docker), projeto da disciplina MO430.

## Estrutura de pastas

Visão geral da raiz do repositório:

```text
mo430-data-pipeline/
├── dags/                      # DAGs e código importado pelo Airflow
│   ├── medallion_pipeline_dag.py
│   ├── .airflowignore         # ignora pacotes que não são DAG
│   └── medallion/             # bronze → silver → gold (Python)
├── data/
│   └── raw/                   # entrada de exemplo (versionada)
│       └── sample.csv
├── include/                   # utilitários / SQL (opcional)
├── plugins/                   # plugins Airflow (opcional)
├── scripts/
│   ├── bootstrap-wsl-venv.sh  # WSL: primeira vez — venv + pip + normaliza LF dos .sh
│   └── wsl-env.sh             # WSL: cada sessão — REPO, AIRFLOW_HOME e ativa o venv
├── requirements.txt
└── README.md
```

| Caminho | Conteúdo |
|---------|----------|
| `dags/` | Arquivos de DAG; o Airflow coloca esta pasta no `PYTHONPATH`. |
| `dags/medallion/` | Funções da pipeline medalhão (`bronze`, `silver`, `gold`). |
| `dags/.airflowignore` | Lista o que o scheduler **não** deve tratar como DAG (ex.: `medallion/`). |
| `data/raw/` | Fonte bruta de exemplo (`sample.csv`). |
| `data/bronze/`, `silver/`, `gold/` | Saídas geradas na execução (criadas automaticamente; no `.gitignore`). |
| `include/`, `plugins/` | Convenção Airflow; podem ficar vazios neste projeto. |
| `scripts/bootstrap-wsl-venv.sh` | WSL: cria o venv em `~/.venvs/`, instala `requirements.txt`, corrige CRLF nos `.sh`. |
| `scripts/wsl-env.sh` | WSL: define `REPO`, `AIRFLOW_HOME` e ativa o venv. |

## Pré-requisitos

- **Windows:** WSL2 com **Ubuntu** (ex.: 24.04). No PowerShell, se ainda não tiver distro: `wsl --install -d Ubuntu-24.04` e reinicie se pedir; confira com `wsl -l -v` (**VERSION** = 2).
- **Python 3.10–3.12** no Ubuntu ([compatibilidade com o Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html)).

Todos os comandos abaixo são no **terminal Ubuntu (WSL)**. O venv **não** vem no Git; cada máquina cria o seu em `~/.venvs/mo430-data-pipeline` (ou outro caminho se definir `MO430_VENV` antes de `source scripts/wsl-env.sh`).

## Instalação

**1.** Pacotes de sistema (uma vez):

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv python3-full
```

**2.** Entre na **raiz do clone** (ajuste `<usuario>` se precisar):

```bash
cd "/mnt/c/Users/<usuario>/OneDrive/Documentos/mestrado/airflow/mo430-data-pipeline"
```

**3.** Criar venv, instalar dependências e corrigir fins de linha dos scripts (recomendado; funciona em `/mnt/c/` / OneDrive):

```bash
bash scripts/bootstrap-wsl-venv.sh
```

Conferir:

```bash
test -f ~/.venvs/mo430-data-pipeline/bin/activate && echo "venv ok"
```

Se o repo estiver só em disco Linux (ex.: `~/projetos/mo430-data-pipeline`), rode o `bash scripts/bootstrap-wsl-venv.sh` na mesma raiz; para venv dentro do repo, use `export MO430_VENV="$PWD/.venv"` antes do `bash scripts/bootstrap-wsl-venv.sh`.

## Uso

**1.** Na raiz do clone, carregue variáveis e venv (Bash):

```bash
cd "/mnt/c/Users/<usuario>/OneDrive/Documentos/mestrado/airflow/mo430-data-pipeline"
source scripts/wsl-env.sh
```

Isso define `REPO`, `AIRFLOW_HOME` e ativa `~/.venvs/mo430-data-pipeline`. Para outro venv, antes do `source`: `export MO430_VENV="$HOME/caminho/do/venv"` (a pasta precisa existir e ter `bin/activate`).

Se aparecer aviso de venv inexistente ou erro de `\r` no Bash, rode de novo na raiz do clone: **`bash scripts/bootstrap-wsl-venv.sh`** (ele recria o venv e normaliza `*.sh` com Python, sem `sed -i` no OneDrive).

**Antes do primeiro `airflow standalone` no WSL**, se já existir `airflow.cfg` / `airflow.db` gerados no Windows na mesma pasta, remova para evitar URL SQLite com `C:/...`:

```bash
rm -f airflow.cfg airflow.db
```

**2.** Subir interface e scheduler (SQLite local):

```bash
airflow standalone
```

**3.** No navegador: **http://localhost:8080** — login com usuário e senha exibidos no terminal (ou `standalone_admin_password.txt` na raiz do projeto).

**4.** Na UI: ative a DAG **`medallion_sample_pipeline`**, depois **Trigger DAG**.

Fluxo das tasks: `bronze_ingest` → `silver_transform` → `gold_aggregate`.

Pela linha de comando:

```bash
airflow dags trigger medallion_sample_pipeline
```

**Arquivos produzidos** (sob `AIRFLOW_HOME` / raiz do clone):

| Etapa | Saída |
|-------|--------|
| Bronze | `data/bronze/bronze_ingest.csv` |
| Silver | `data/silver/silver_clean.csv` |
| Gold | `data/gold/gold_por_categoria.csv` |

## DAG de exemplo (resumo)

| Camada | Arquivo | Ação |
|--------|---------|------|
| Bronze | `dags/medallion/bronze.py` | Lê `data/raw/sample.csv`, grava bronze com `ingested_at`. |
| Silver | `dags/medallion/silver.py` | Lê bronze, deduplica por `id`, normaliza, grava silver. |
| Gold | `dags/medallion/gold.py` | Lê silver, agrega por `categoria`, grava gold. |

## Comandos úteis

```bash
airflow dags list
airflow dags list-import-errors
```

## Modo manual (opcional)

Com venv ativo e `AIRFLOW_HOME` definido: `airflow db migrate`, crie usuário com `airflow users create ...`, depois um terminal com `airflow scheduler` e outro com `airflow webserver --port 8080`.

## Notas

- Dependências: apenas `requirements.txt` (sem Docker).
- `data/raw/sample.csv` é versionado; `data/bronze|silver|gold/` são artefatos locais (`.gitignore`).
