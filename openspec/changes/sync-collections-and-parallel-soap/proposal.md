# Proposal: Sincronização Direta de Coleções e Vínculos Institucionais + Download Paralelo com TTL

## Why

1. O campo `researcher.institution_id` foi descontinuado em prol de relacionamentos N:N em `researcher_institution`. O ETL precisa sincronizar instituições e vínculos diretamente do `admin_db` para `public.institution` e `public.researcher_institution` no `write_db`, sem passar por colunas legadas (`institution_id`, `status`, `extra_field`).
2. O ecossistema agora suporta coleções (`collection` e `researcher_collection`), permitindo pesquisadores com frequências distintas de atualização (1 dia para `simcc`, 30 dias para `fomento`).
3. O download de XML via SOAP era 100% sequencial e não verificava a idade do arquivo contra o intervalo da coleção. Para processar os 43.771 pesquisadores, o download precisa ser paralelizado com pool de conexões e respeitar o TTL baseado em `refresh_interval_days`.

## What Changes

- `soap_lattes.py`:
  - Lê `lattes_id` e `refresh_interval_days` do Parquet exportado pelo `simcc-admin`.
  - Verifica a idade do XML em `storage/xml/current/{lattes_id}.xml`. Se a idade for menor que `refresh_interval_days`, usa o cache.
  - Paraleliza o download com `ThreadPoolExecutor` e pool de conexões HTTP.
- `sanitize_lattes_data.py`:
  - Remove o passo legado que lia de `researcher.institution_id`.
  - Adiciona sincronização direta a partir do `admin_db` (`settings.ADMIN_DATABASE_URL`) para `public.institution`, `public.researcher_institution`, `public.collection` e `public.researcher_collection`.
- `publish_to_read_db.py`:
  - Já replica todas as tabelas públicas do schema, portanto passa a replicar automaticamente `collection` e `researcher_collection` via `COPY BINARY`.
- `Jade-Extrator-Hop`:
  - Remove a escrita em `researcher.institution_id`, `status` e `extra_field` no pipeline de importação de pesquisadores.

## Capabilities

- `parallel-soap-ttl`: Download paralelo de currículos com cache respeitando `refresh_interval_days`.
- `sync-collections-and-institutions`: Sincronização direta de coleções e instituições N:N entre `admin_db` e `write_db`.
