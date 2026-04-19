# mo430-data-pipeline

Pipeline de dados com **Apache Airflow** para orquestração, ingestão e processamento dos dados do projeto da disciplina MO430.

## Estrutura do repositório

| Caminho | Função |
|--------|--------|
| `dags/` | Definições de DAGs e pacote `medallion/` (camadas **bronze**, **silver**, **gold**) |
| `dags/.airflowignore` | Evita que o Airflow trate a pasta `medallion/` como arquivos de DAG |
| `include/` | Pasta reservada para utilitários ou SQL (vazia por padrão) |
| `plugins/` | Plugins Airflow (vazio por padrão) |
| `data/raw/` | Fonte bruta de exemplo (`sample.csv`) |
| `data/bronze/`, `data/silver/`, `data/gold/` | Saídas geradas ao rodar a DAG |

O código compartilhado da pipeline fica em `dags/medallion/` porque essa área entra no `PYTHONPATH` do Airflow junto com a pasta de DAGs, o que evita erros de import.

## Por que WSL2 no Windows?

O Airflow **não é suportado no Windows nativo** para subir o webserver (erros como `No module named 'pwd'` são comuns). A forma mais simples de desenvolver no Windows é usar **WSL2** com **Ubuntu**, manter o projeto no disco `C:` (acessível em `/mnt/c/...`) e rodar **Python, venv e Airflow dentro do Linux**.

Este projeto **não** usa Docker; dependências vêm só do `pip` e do `requirements.txt`.

## Pré-requisitos (Windows)

- Windows 10/11 com **WSL2** e uma distro **Ubuntu** (ex.: Ubuntu 24.04).
- **Python 3.10–3.12** dentro do Ubuntu ([matriz de versões do Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html)).

### Instalar WSL e Ubuntu (uma vez)

No **PowerShell** (pode precisar de administrador na primeira vez):

```powershell
wsl --install -d Ubuntu-24.04
```

Reinicie se o Windows pedir. Depois abra **Ubuntu** no menu Iniciar e crie usuário/senha Linux quando solicitado.

Confira se está em WSL **2**:

```powershell
wsl -l -v
```

A coluna **VERSION** deve ser **2** para `Ubuntu-24.04` (ou o nome da sua distro).

## Fluxo local: WSL → ambiente virtual → instalação → Airflow

Tudo abaixo roda no **terminal Ubuntu** (ou `wsl -d Ubuntu-24.04` a partir do PowerShell). Ajuste o caminho se o clone não estiver em `OneDrive/.../mo430-data-pipeline`.

### 1. Pacotes do sistema no Ubuntu (uma vez)

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
```

### 2. Ir à raiz do repositório

Exemplo (troque `vitor` pelo seu usuário Windows se for diferente):

```bash
cd "/mnt/c/Users/vitor/OneDrive/Documentos/mestrado/airflow/mo430-data-pipeline"
```

### 3. Ambiente virtual **no Linux**

O `.venv` criado no Windows **não** deve ser reutilizado no WSL (pastas `Scripts/` vs `bin/`). Dentro do Ubuntu, na raiz do projeto:

```bash
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

**Se aparecer** `bash: .venv/bin/activate: No such file or directory`:

1. Confirme que está na raiz do clone (`ls` deve mostrar `requirements.txt` e `dags/`).
2. Rode `ls .venv` (se der “No such file”, o venv ainda não existe: execute **só** `python3 -m venv .venv` e depois `source .venv/bin/activate`).
3. Se existir **`.venv/Scripts/`** em vez de **`.venv/bin/`**, esse venv foi criado no **Windows**. Apague e recrie **no Ubuntu**: `rm -rf .venv`, depois os comandos do bloco acima a partir de `python3 -m venv .venv`.

### 4. `AIRFLOW_HOME` na raiz do clone

O Airflow grava `airflow.db`, `logs/` e configuração em `AIRFLOW_HOME`. Use **a própria raiz do repositório** (onde estão `dags/`, `data/`, `include/`).

```bash
export AIRFLOW_HOME="$(pwd)"
echo "$AIRFLOW_HOME"
```

Em cada **nova** sessão de terminal: `cd` de novo, `source .venv/bin/activate` e `export AIRFLOW_HOME="$(pwd)"`.

### 5. Subir o Airflow e abrir a interface

```bash
airflow standalone
```

Aguarde a mensagem com **usuário e senha** de admin (pode existir também `standalone_admin_password.txt` nessa pasta).

No **navegador do Windows**, abra: **http://localhost:8080** e faça login.

### 6. Próximas vezes (atalho)

```bash
cd "/mnt/c/Users/vitor/OneDrive/Documentos/mestrado/airflow/mo430-data-pipeline"
source .venv/bin/activate
export AIRFLOW_HOME="$(pwd)"
airflow standalone
```

## DAG medalhão de exemplo

- **DAG ID:** `medallion_sample_pipeline`
- **Fluxo:** `bronze_ingest` → `silver_transform` → `gold_aggregate`

Ative a DAG na UI e use **Trigger DAG** (ou, com o venv ativo e `AIRFLOW_HOME` definido):

```bash
airflow dags trigger medallion_sample_pipeline
```

### O que cada camada faz (arquivos)

1. **Bronze** (`dags/medallion/bronze.py`): lê `data/raw/sample.csv` e grava `data/bronze/bronze_ingest.csv` com coluna `ingested_at`.
2. **Silver** (`dags/medallion/silver.py`): lê a bronze, remove duplicata por `id`, normaliza texto e valor, grava `data/silver/silver_clean.csv`.
3. **Gold** (`dags/medallion/gold.py`): lê a silver e grava `data/gold/gold_por_categoria.csv` com totais por `categoria`.

## Comandos úteis

```bash
airflow dags list
airflow dags list-import-errors
```

Se a DAG não carregar, o segundo comando mostra o traceback.

## Modo manual (opcional, ainda no WSL)

Dois terminais Ubuntu, com venv ativo e `AIRFLOW_HOME` definido. Na primeira vez (ou após apagar `airflow.db`):

```bash
airflow db migrate
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

Terminal 1: `airflow scheduler`  
Terminal 2: `airflow webserver --port 8080`  
Acesse **http://localhost:8080**.

## Observações

- Pastas `data/bronze/`, `data/silver/` e `data/gold/` estão no `.gitignore`; o arquivo **`data/raw/sample.csv`** continua versionado como entrada de exemplo.
