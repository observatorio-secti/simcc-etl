# SimCC ETL

Serviço de extração, transformação e carga (ETL) desacoplado do ecossistema SIMCC, projetado para execução independente, alta performance, rastreabilidade completa e integração segura com o banco administrativo e de leitura.

---

## 🏗 Arquitetura

O `simcc-etl` opera com isolamento de responsabilidades entre escrita e leitura:

```
[SimCC Admin API] (Exportação Parquet via /academic/researchers/export/parquet)
       │ (HTTP + X-Service-Token)
       ▼
[SimCC ETL Pipeline]
  ├── Pre-Hop: Seeding, Import CAPES, SOAP Lattes (Consome Parquet do Admin + Proxy UESC)
  ├── Hop Execution: Docker Jade-Extrator (Index.hwf)
  ├── Post-Hop: SQL Sanitization, Researcher Production, IndProd (Pesquisador / Grupo / PG)
  └── Enrichments: Classificação, Dicionários, OpenAlex
       │
       ▼ (Advisory Lock: pg_try_advisory_lock(9001001))
[WRITE_DB] (Escrita dedicada / Processamento pesado)
       │
       ▼ (psycopg binary COPY + REFRESH MATERIALIZED VIEW + Cache Bump)
[READ_DB] (Banco somente-leitura consumido pela API pública)
```

### Principais Características

1. **Lock Concorrente Robusto**: `pg_try_advisory_lock(9001001)` no PostgreSQL evita execuções simultâneas por CLI ou API.
2. **Logs Estruturados JSONL**: Cada execução possui seu próprio log (`logs/runs/{run_id}.jsonl`) e log diário (`logs/{YYYY-MM-DD}.jsonl`).
3. **Apache Hop via Docker SDK**: O container `gleidsoncosta/simcc-extrator:latest` roda de forma isolada, montando os volumes do projeto.
4. **Proxy CNPq Automático**: Utiliza a rota alternativa do proxy (`https://simcc.uesc.br/v3/api`) quando o webservice oficial do CNPq está inacessível.
5. **Cálculo de IndProd Unificado**: Barema tipado, separação estrita de patentes concedidas/não-concedidas, cálculo para pesquisadores, programas de pós e grupos de pesquisa.
6. **Publicação Atômica**: `psycopg` binary COPY com `session_replication_role = 'replica'`, atualização das 15 visões materializadas e invalidação de cache via incremento de geração no Redis (`simcc:v2:search:generation`).

---

## ⚙️ Configuração (`.env`)

| Variável | Obrigatória | Descrição |
|---|:---:|---|
| `WRITE_DATABASE_URL` | Sim | Conexão com o banco de escrita e processamento do ETL (`simcc_write`) |
| `READ_DATABASE_URL` | Sim | Conexão com o banco de leitura pública (`simcc`) |
| `ADMIN_DATABASE_URL` | Sim | Conexão com o banco do admin (usada para workflows JDBC do Hop) |
| `ADMIN_API_URL` | Sim | URL base da API do `simcc-admin` (padrão: `http://localhost:8009` em dev) |
| `ADMIN_SERVICE_TOKEN` | Sim | Chave de serviço (*M2M*) para autenticação no `simcc-admin` via `X-Service-Token` |
| `REDIS_URL` | Sim | Conexão Redis para bump de cache geracional |
| `ETL_API_KEY` | Sim | Chave de autenticação da API REST do ETL (`X-ETL-API-Key`) |
| `ALTERNATIVE_CNPQ_SERVICE` | Não | Habilita uso do proxy UESC em vez do WSDL oficial do CNPq (padrão: `true`) |
| `CNPQ_PROXY_URL` | Não | Endpoint do proxy REST do CNPq (`https://simcc.uesc.br/v3/api`) |
| `SOAP_MAX_WORKERS` | Não | Quantidade máxima de workers em paralelo no download do Lattes (padrão: `16`) |

---

## 📁 Estrutura de Pastas

```
simcc-etl/
├── simcc_etl/
│   ├── core/
│   │   ├── database.py       # Engines SQLAlchemy, modelos EtlRun e ResearchGroupIndProd
│   │   ├── logging.py        # Logs estruturados structlog / jsonl
│   │   ├── settings.py       # Pydantic Settings
│   │   └── text.py           # Normalização de texto e caracteres
│   ├── routines/             # Módulos desacoplados de cada rotina
│   │   ├── soap_lattes.py
│   │   ├── extract_lattes_hop.py
│   │   ├── import_capes_programs.py
│   │   ├── sanitize_lattes_data.py
│   │   ├── aggregate_researcher_production.py
│   │   ├── calculate_production_index.py
│   │   ├── classify_researchers.py
│   │   ├── build_search_dictionary.py
│   │   ├── resolve_cnpq_lattes_id.py
│   │   ├── enrich_from_openalex.py
│   │   ├── import_program_research_lines.py
│   │   ├── export_oda_research_groups.py
│   │   ├── classify_article_themes.py
│   │   └── publish_to_read_db.py
│   ├── app.py                # Aplicação FastAPI
│   ├── cli.py                # Interface de linha de comando
│   ├── pipeline.py           # Orquestrador com advisory lock
│   └── routers.py            # Endpoints REST (/routines, /runs, /logs)
├── storage/
│   ├── seed/                 # Arquivos CSV de seed CAPES
│   ├── xml/                  # Cache de XMLs Lattes
│   └── openalex/             # Cache de respostas OpenAlex
├── tests/
│   ├── unit/
│   └── integration/
├── compose.yaml
├── Dockerfile
└── pyproject.toml
```

---

## 🚀 Como Executar

### 1. Pré-requisitos
- Python 3.12+ (recomendado 3.13)
- Poetry (`pip install poetry`)
- Docker & Docker Compose

### 2. Instalação de Dependências
```bash
poetry install
```

### 3. Subir Bancos e Serviços com Docker Compose
```bash
docker compose up -d write_db read_db redis
```

### 4. Execução via CLI

Listar rotinas disponíveis:
```bash
poetry run python -m simcc_etl.cli list
```

Executar uma rotina específica (ex: Seeding do Hop):
```bash
poetry run python -m simcc_etl.cli run hop_seed
```

Executar download de currículos via SOAP Lattes com filtro de pesquisadores:
```bash
poetry run python -m simcc_etl.cli run soap_lattes --lattes-ids 1966167015825708 6716225567627323
```

Executar publicação para o banco de leitura:
```bash
poetry run python -m simcc_etl.cli run publish_to_read_db
```

### 5. Execução via API REST

Iniciar servidor FastAPI:
```bash
poetry run task run
```

Endpoints disponíveis:
- `GET /routines`: Lista todas as rotinas cadastradas.
- `POST /runs`: Dispara uma rotina ou pipeline completo (`{"routine_name": "...", "lattes_ids": [...]}`).
- `GET /runs`: Histórico de execuções com status, contadores e duração.
- `GET /runs/{run_id}`: Detalhes de uma execução.
- `GET /runs/{run_id}/logs?offset=0`: Acompanhamento em streaming/polling dos logs estruturados.

---

## 🛠 Tarefas de Desenvolvimento (Taskipy)

- `poetry run task lint`: Executa verificação de código com Ruff.
- `poetry run task format`: Formata o código respeitando as convenções do projeto.
- `poetry run task test`: Executa a suíte de testes com cobertura.
