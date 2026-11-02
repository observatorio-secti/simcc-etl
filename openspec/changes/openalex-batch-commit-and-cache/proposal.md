# Proposal: Commit em Lotes e Cache em Disco para Rotina OpenAlex

## Why

1. A rotina `enrich_from_openalex` realizava o `session.commit()` apenas no encerramento de todo o processamento de artigos e pesquisadores. Em volumes de milhares de registros, se o processo fosse interrompido (SIGINT/Ctrl+C, encerramento de container, timeout ou falha fatal), o banco de dados realizava rollback total de toda a transação, perdendo todas as inserções realizadas.
2. A rotina gravava o arquivo JSON em disco (`storage/openalex/article/` e `storage/openalex/researcher/`), mas não checava se o arquivo já existia antes de disparar uma nova requisição HTTP para a API pública do OpenAlex. Ao reiniciar um processo interrompido, a rotina refazia todas as requisições de rede desnecessariamente.
3. É necessário garantir persistência incremental no banco de dados a cada 50 registros salvos com sucesso e reaproveitamento do cache local em disco para evitar requisições redundantes e respeitar limites de taxa da API OpenAlex.

## What Changes

- `simcc_etl/core/settings.py`:
  - Adiciona `OPENALEX_BATCH_COMMIT_SIZE: int = 50` configurável via variável de ambiente.
- `simcc_etl/routines/enrich_from_openalex.py`:
  - Adiciona suporte a batch commit a cada 50 registros processados com sucesso (`batch_size`), além de commit residual garantido ao final do processamento.
  - Adiciona verificação prévia da existência do arquivo JSON em `storage/openalex/article/{article_id}.json` e `storage/openalex/researcher/{researcher_id}.json`. Caso já exista e seja válido, utiliza o payload em disco em vez de bater na API externa.
  - Implementa tratamento isolado de exceções com `session.rollback()` no bloco de processamento do registro para evitar contaminação da sessão SQLAlchemy.
- `.env.example` e `README.md`:
  - Documentação da variável `OPENALEX_BATCH_COMMIT_SIZE` e do comportamento de commit incremental e cache local.

## Capabilities

- `openalex-batch-commit`: Persistência transacional a cada 50 registros com sucesso e commit residual no encerramento da rotina.
- `openalex-disk-cache`: Leitura e reaproveitamento de respostas salvas localmente em disco em `storage/openalex/`.
