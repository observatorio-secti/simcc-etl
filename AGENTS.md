# AGENTS.md — SimCC ETL (`simcc-etl`)

Este repositório faz parte do **Ecossistema Observatório CT&I Bahia / SimCC**. Ele é versionado de forma independente e atua como o **orquestrador do pipeline ETL** (via CLI e API FastAPI na porta `8001`), coordenando a extração de currículos Lattes, execução do container Apache Hop (`Jade-Extrator-Hop`), importação CAPES, sanitização, cálculo de `IndProd`, enriquecimento e publicação atômica do `write_db` (`5435`) para o `read_db` (`5434`).

> Para o guia completo de integração entre `simcc-admin`, `simcc-etl`, `Jade-Extrator-Hop` e `simcc-back`, consulte `../README.md` (`Simcc/README.md`) e `../../AGENTS.md`.

---

## 1. Princípios Arquiteturais e Contratos de Dados

1. **Isolamento de Bancos (`admin_db`, `write_db`, `read_db`) e Lock Concorrente**:
   - Lê cadastros de pesquisadores/instituições de `ADMIN_DATABASE_URL` (`5433`).
   - Executa todo o processamento pesado em `WRITE_DATABASE_URL` (`5435`), protegido por `pg_try_advisory_lock(9001001)`.
   - Publica os dados prontos em `READ_DATABASE_URL` (`5434`) usando `psycopg` binary `COPY` com `session_replication_role = 'replica'`, atualiza as 15 *Materialized Views* e incrementa a geração de cache no Redis (`simcc:v2:search:generation`).
2. **Dependência Cruzada de Migrações com `simcc-back`**:
   - As 76 tabelas de domínio base e as 15 *Materialized Views* pertencem às migrações do **`simcc-back`**.
   - As migrações deste repositório (`migrations/`) usam `version_table = 'alembic_version_etl'` e assumem que o schema do `simcc-back` já foi aplicado tanto no `write_db` quanto no `read_db`.
3. **Ordem Estrita das 12 Rotinas Padrão (`DEFAULT_ROUTINES`)**:
   - `soap_lattes` $\rightarrow$ `hop_index` $\rightarrow$ `import_capes_programs` $\rightarrow$ `import_capes_ratings` $\rightarrow$ `import_capes_researchers` $\rightarrow$ `sanitize_lattes_data` $\rightarrow$ `aggregate_researcher_production` $\rightarrow$ `calculate_production_index` $\rightarrow$ `classify_researchers` $\rightarrow$ `build_search_dictionary` $\rightarrow$ `resolve_cnpq_lattes_id` $\rightarrow$ `publish_to_read_db`.
   - As rotinas CAPES devem rodar **após** `hop_index` (que importa as instituições) e **antes** de `sanitize_lattes_data` (que calcula a flag `docente`).
4. **Idioma e Convenções**:
   - Documentação, comentários, logs de domínio e commits em **Português do Brasil (pt-BR)**.
   - Identificadores de código em **inglês**.

---

## 2. Comandos Padrão de Desenvolvimento

- **Instalar dependências**: `poetry install`
- **Subir bancos e Redis**: `docker compose up -d write_db read_db redis`
- **Aplicar migrações complementares do ETL**: `poetry run alembic upgrade head`
- **Listar/executar rotinas via CLI**:
  - `poetry run python -m simcc_etl.cli --routines hop_seed`
  - `poetry run python -m simcc_etl.cli --lattes-ids <id1> <id2>`
- **Iniciar API REST do ETL**: `poetry run task run`
- **Testes, lint e formatação**: `poetry run task test`, `poetry run task lint`, `poetry run task format`

---

## 3. Padrão de Documentação e Gatilhos Obrigatórios (Definition of Done)

A documentação deste serviço vive em [`README.md`](README.md) e no guia de integração do subsistema em `../README.md` (`Simcc/README.md`).

### Tabela de Atualização Obrigatória

**Nenhuma alteração de código está pronta sem atualizar a documentação correspondente na mesma tarefa:**

| Se você alterar... | Atualize obrigatoriamente: |
| :--- | :--- |
| Variáveis em `simcc_etl/core/settings.py` | `.env.example` (se existir), `README.md` e a seção de `.env` em `../README.md` |
| Rotinas em `simcc_etl/routines/` ou ordem em `simcc_etl/pipeline.py` | Árvore de pastas e lista de rotinas em [`README.md`](README.md) e na seção 4.3 de `../README.md` |
| Argumentos da CLI (`simcc_etl/cli.py`) ou endpoints (`simcc_etl/routers.py`) | Seções **4. Execução via CLI** e **5. Execução via API REST** no `README.md` |
| Tabelas copiadas ou MVs atualizadas em `publish_to_read_db.py` | `README.md` e `../README.md` |
