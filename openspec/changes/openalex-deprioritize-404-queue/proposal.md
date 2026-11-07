# Proposal: Priorização de Registros Inéditos e Fim de Fila para 404 no OpenAlex

## Why

1. Atualmente, os artigos (DOI) e pesquisadores (ORCID) que resultam em resposta `404 Not Found` na API do OpenAlex não deixam registro no banco nem arquivo em disco.
2. A cada reinicialização ou nova execução do pipeline de ETL, a query SQL `WHERE oa.article_id IS NULL` retorna novamente esses mesmos registros no início da fila de processamento.
3. Como resultado, o sistema gasta tempo, banda e cota de requisições retestando DOIs inexistentes repetidamente antes mesmo de tentar registros novos que nunca foram consultados.
4. É necessário registrar as ocorrências de 404 em cache de falhas local (`not_found`) e priorizar 100% dos registros inéditos, empurrando as retentativas de registros que já deram 404 para o final da fila de execução.

## What Changes

- `simcc_etl/routines/enrich_from_openalex.py`:
  - Adiciona diretórios de cache de falhas: `storage/openalex/not_found/article/` e `storage/openalex/not_found/researcher/`.
  - Registra arquivos sentinela com metadados (`status_code: 404`, `attempted_at`) quando a API responder 404.
  - Implementa particionamento de fila em memória: prioriza os registros nunca tentados (inéditos) no topo da fila e desloca os que possuem registro de 404 para o fim da fila.
  - Em caso de sucesso (200 OK) em uma retentativa, remove o registro antigo de `not_found`.
- `tests/unit/test_enrich_from_openalex.py`:
  - Testes unitários para particionamento de fila (inéditos vs 404).
  - Testes unitários para persistência do marcador de 404 em `not_found` e limpeza em caso de recuperação (200 OK).
- `README.md`:
  - Atualização da documentação da rotina OpenAlex descrevendo o particionamento de fila e o cache de 404.

## Capabilities

- `openalex-queue-partitioning`: Priorização de registros inéditos e deslocamento de itens 404 para o fim da fila.
- `openalex-negative-caching`: Armazenamento isolado de tentativas 404 em `storage/openalex/not_found/`.
