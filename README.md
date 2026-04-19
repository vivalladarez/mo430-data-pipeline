# mo430-data-pipeline

Pipeline de dados com **Apache Airflow** para orquestração, ingestão e processamento dos dados do projeto da disciplina MO430.

## Estrutura do repositório

| Caminho | Função |
|--------|--------|
| `dags/` | Definições de DAGs e pacote `medallion/` (camadas **bronze**, **silver**, **gold**) |
| `include/` | Pasta reservada para utilitários ou SQL (vazia por padrão) |
| `plugins/` | Plugins Airflow (vazio por padrão) |
| `data/raw/` | Fonte bruta de exemplo (`sample.csv`) |
| `data/bronze/`, `data/silver/`, `data/gold/` | Saídas geradas ao rodar a DAG|

## Pré-requisitos

- **Python 3.10, 3.11 ou 3.12** (alinhar com a [matriz de versões](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html) do Airflow que você instalar).
- Ferramenta de linha de comando na raiz do repositório.

## Instalação

### 1. Ambiente virtual (recomendado)

**Windows (PowerShell)**

```powershell
cd "c:\Users\vitor\OneDrive\Documentos\mestrado\airflow\mo430-data-pipeline"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

**Linux / macOS**

```bash
cd /caminho/para/mo430-data-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Definir `AIRFLOW_HOME` na raiz do projeto

O Airflow grava configuração, metadados e **logs** neste diretório. Para este repositório, use **a própria raiz do clone** (onde estão `dags/`, `include/`, `data/`). O código compartilhado da pipeline fica em `dags/medallion/` porque essa pasta entra automaticamente no `PYTHONPATH` do Airflow (junto com `dags/`), o que evita erros de import na DAG.

**Windows (PowerShell), na mesma sessão em que você rodará o Airflow:**

```powershell
$env:AIRFLOW_HOME = (Get-Location).Path
```

**Linux / macOS**

```bash
export AIRFLOW_HOME="$(pwd)"
```

> Em cada novo terminal, defina `AIRFLOW_HOME` de novo **ou** persista a variável no seu perfil de shell.

## Subir o Airflow e ver a interface local

### Modo rápido: `airflow standalone`

Cria/atualiza o banco SQLite, sobe **scheduler** e **webserver** e imprime credenciais de admin (também pode gerar `standalone_admin_password.txt` em `AIRFLOW_HOME`).

```powershell
# PowerShell (com venv ativo e AIRFLOW_HOME definido)
airflow standalone
```

Depois abra no navegador: **http://localhost:8080** e faça login com o usuário e senha mostrados no terminal.

### Modo “manual” (dois terminais)

Terminal 1 — banco e migrações (só na primeira vez ou após apagar `airflow.db`):

```powershell
airflow db migrate
airflow users create `
  --username admin `
  --firstname Admin `
  --lastname User `
  --role Admin `
  --email admin@example.com `
  --password admin
```

Terminal 1 — scheduler:

```powershell
airflow scheduler
```

Terminal 2 — interface web:

```powershell
airflow webserver --port 8080
```

Acesse **http://localhost:8080**.

## DAG medalhão de exemplo

- **DAG ID:** `medallion_sample_pipeline`
- **Fluxo:** `bronze_ingest` → `silver_transform` → `gold_aggregate`

Ative a DAG na UI e use **Trigger DAG** (ou `airflow dags trigger medallion_sample_pipeline`).

### O que cada camada faz (arquivos)

1. **Bronze** (`dags/medallion/bronze.py`): lê `data/raw/sample.csv` e grava `data/bronze/bronze_ingest.csv` com coluna `ingested_at`.
2. **Silver** (`dags/medallion/silver.py`): lê a bronze, remove duplicata por `id`, normaliza texto e valor, grava `data/silver/silver_clean.csv`.
3. **Gold** (`dags/medallion/gold.py`): lê a silver e grava `data/gold/gold_por_categoria.csv` com totais por `categoria`.

## Comandos úteis

```powershell
airflow dags list
airflow dags list-import-errors
```

Se houver erro de import na DAG, o segundo comando mostra o traceback.

## Observações

- Este projeto **não** usa Docker; dependências vêm só do `pip` e do `requirements.txt`.
- Pastas `data/bronze/`, `data/silver/` e `data/gold/` estão no `.gitignore` para não versionar artefatos gerados; o arquivo de entrada **`data/raw/sample.csv`** permanece no repositório como exemplo.
