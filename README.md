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
  ├── Hop Execution: Hop Server long-lived Jade-Extrator (Index.hwf via REST)
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
3. **Apache Hop Server long-lived**: o serviço `hop` (`gleidsoncosta/simcc-extrator:latest`, base `apache/hop`) fica no ar e o ETL dispara os workflows pela API REST. Não há socket Docker nem container efêmero.
4. **Proxy CNPq Automático**: Utiliza a rota alternativa do proxy (`https://simcc.uesc.br/v3/api`) quando o webservice oficial do CNPq está inacessível.
5. **Cálculo de IndProd Unificado**: Barema tipado, separação estrita de patentes concedidas/não-concedidas, cálculo para pesquisadores, programas de pós e grupos de pesquisa.
6. **Publicação Atômica**: `psycopg` binary COPY com `session_replication_role = 'replica'`, atualização das 15 visões materializadas e invalidação de cache via incremento de geração no Redis (`simcc:v2:search:generation`).
7. **Enriquecimento Incremental com OpenAlex**: Commit em lotes a cada 50 registros de sucesso com cache local em disco (`storage/openalex/`), isolamento transacional via savepoints (`begin_nested`), priorização de registros inéditos e deslocamento de itens com resposta 404 para o fim da fila (`storage/openalex/not_found/`).

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
| `OPENALEX_API_KEY` | Não | Chave de autenticação da API OpenAlex para limites de taxa ampliados |
| `OPENALEX_BATCH_COMMIT_SIZE` | Não | Tamanho do lote de registros com sucesso para commit no banco na rotina OpenAlex (padrão: `50`) |
| `HOP_SERVER_URL` | Não | URL do Hop Server (padrão: `http://localhost:8080`, para o ETL via Poetry; com o ETL em container: `http://hop:8080`, já injetado pelo `compose.yaml`) |
| `HOP_SERVER_USER` / `HOP_SERVER_PASSWORD` | Sim em produção | Credenciais Basic do Hop Server (padrão: `cluster`/`cluster`). O `compose.yaml` repassa os mesmos valores ao serviço `hop` (`HOP_SERVER_PASS` na imagem) |
| `HOP_SERVER_TIMEOUT` | Não | Tempo máximo, em segundos, de uma execução síncrona de workflow (padrão: `21600`, ou seja, 6h) |
| `HOP_WRITE_JDBC_URL` / `HOP_ADMIN_JDBC_URL` | Sim | URLs JDBC usadas **de dentro do Hop Server**, com hosts da rede `simcc-network` (ex.: `write_db`, `admin_db`). São enviadas como variáveis `DATABASE_URL`/`ADMIN_DATABASE_URL` a cada execução |
| `STORAGE_DIR` | Não | Pasta de trabalho do ETL: XMLs em `xml/current`, `xml/run_current` e `xml/hop_input`, CSVs CAPES em `seed/` e cache OpenAlex. Padrão: `storage` (relativo ao diretório de execução) |
| `STORAGE_PATH` | Não | Lido apenas pelo `compose.yaml`: pasta do host montada como storage do `etl` (`/app/storage`), cuja subpasta `xml/hop_input` é montada no `hop` (padrão: `./storage`) |
| `HOP_SERVER_PORT` | Não | Lido apenas pelo `compose.yaml`: porta do Hop Server publicada em `127.0.0.1` (padrão: `8080`; mantenha `HOP_SERVER_URL` coerente) |
| `ETL_PORT` | Não | Lido apenas pelo `compose.yaml`: porta do host publicada em `127.0.0.1` para a API (padrão: `8001`) |

### Apache Hop Server e pasta de XMLs

As rotinas `hop_seed` e `hop_index` chamam o Hop Server, um container permanente (serviço `hop` do `compose.yaml`) que carrega o projeto `Jade-Extrator` ao subir:

1. O ETL confere o servidor em `/hop/status`. Se ele estiver inacessível ou recusar as credenciais, a rotina falha com mensagem explicando o que ajustar.
2. Executa `/hop/execWorkflow/?workflow=${PROJECT_HOME}/workflows/<X>.hwf`, que é **síncrono**.
3. Esse endpoint responde `OK` mesmo quando o workflow falha e não devolve o id da execução. Por isso o ETL localiza a execução nova em `/hop/status`, lê o status final e o log em `/hop/workflowStatus` e falha se o status não for `Finished` ou se o log tiver erro fatal (` - ERROR`, exceto `ERROR: DUP`, ou `result=[false]`). O log do Hop é repassado ao log estruturado da execução.
4. Remove a execução do servidor (`/hop/removeWorkflow`) para não acumular memória.

Os pipelines do Hop leem sempre `${PROJECT_HOME}/datasets/lattes_xml/`, um caminho fixo em dezenas de `.hpl`. O `compose.yaml` monta nele a pasta `STORAGE_DIR/xml/hop_input`. Antes de cada `hop_index`, o ETL esvazia o **conteúdo** dessa pasta, sem recriá-la (recriar deixaria órfão o bind mount do `hop`), e a preenche com **hardlinks** dos XMLs de `xml/run_current` (quando há `lattes_ids` e a pasta tem XMLs) ou de `xml/current`. Se os dois não estiverem no mesmo sistema de arquivos, o ETL copia. Se a pasta de origem não existir ou não tiver XMLs, `hop_index` falha antes de chamar o Hop. `hop_seed` não usa XMLs.

> A pasta `xml/hop_input` precisa existir **antes** do `docker compose up hop`. Do contrário, o Docker a cria como `root` e o ETL não consegue preenchê-la.

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
│   ├── xml/                  # XMLs Lattes: current/, run_current/ e hop_input/ (lida pelo Hop Server)
│   ├── openalex/             # Cache de respostas OpenAlex (article/, researcher/, not_found/)
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

O Hop Server (serviço `hop`) também é necessário para `hop_seed` e `hop_index`. A imagem é construída a partir de `../Jade-Extrator-Hop`:
```bash
docker build -t gleidsoncosta/simcc-extrator:latest ../Jade-Extrator-Hop
mkdir -p storage/xml/hop_input
docker compose up -d hop
```
Depois de reconstruir a imagem (alterações em `.hwf`/`.hpl`), recrie o servidor com `docker compose up -d --force-recreate hop`: o Hop Server carrega o projeto só ao subir.

**Opcional: API do ETL em container.** O serviço `etl` (perfil `etl`) só sobe quando pedido explicitamente. Ele monta `STORAGE_PATH` em `/app/storage` e fala com o Hop por `http://hop:8080`. Use hosts da rede `simcc-network` nas URLs de banco e Redis (ex.: `write_db:5432`, não `localhost`). A pasta precisa ser gravável pelo uid `1000` (`appuser` da imagem):
```bash
sudo chown -R 1000:1000 storage
docker compose --profile etl up -d --build etl
```
A API fica em `http://127.0.0.1:8001`.

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
- `GET /dashboard` (ou `/`): Interface web interativa para monitorar execuções em tempo real, etapas e logs estruturados.
- `GET /routines`: Lista todas as rotinas cadastradas.
- `POST /runs`: Dispara uma rotina ou pipeline completo (`{"routine_name": "...", "lattes_ids": [...]}`).
- `GET /runs`: Histórico de execuções com status, contadores e duração.
- `GET /runs/{run_id}`: Detalhes de uma execução.
- `GET /runs/{run_id}/logs?offset=0`: Acompanhamento em streaming/polling dos logs estruturados.

### 6. Monitoramento Visual via Dashboard

O serviço inclui uma interface web integrada para acompanhamento ao vivo das rotinas e histórico de logs:
- Acesse `http://localhost:8001/dashboard` (ou `http://localhost:8001/` que redireciona automaticamente).
- Exibe o status da execução atual (`RUNNING`, `SUCCEEDED`, `FAILED`), a rotina em andamento, o tempo decorrido e a régua de progresso das etapas.
- Permite alternar entre execuções individuais e logs diários consolidados, com filtros por nível (`INFO`, `WARN`, `ERROR`), rotina, busca textual e download do arquivo `.jsonl`.

---

## 🛠 Tarefas de Desenvolvimento (Taskipy)

- `poetry run task lint`: Executa verificação de código com Ruff.
- `poetry run task format`: Formata o código respeitando as convenções do projeto.
- `poetry run task test`: Executa a suíte de testes com cobertura.
