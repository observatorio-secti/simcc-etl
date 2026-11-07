# Design: Commit em Lotes e Cache em Disco para Rotina OpenAlex

## Decisões

1. **Commit em Lotes por Contador de Sucessos (`batch_size = 50`)**:
   - Manter um contador `uncommitted_count` incrementado a cada registro (artigo ou pesquisador) inserido com sucesso no banco.
   - Sempre que `uncommitted_count >= batch_size`, dispara `session.commit()` e zera o contador.
   - Ao final do processamento (após os loops de artigos e pesquisadores), se `uncommitted_count > 0`, dispara `session.commit()` para salvar os registros residuais.
   - `batch_size` tem valor padrão configurável via `settings.OPENALEX_BATCH_COMMIT_SIZE` (padrão: 50) e pode ser sobrescrito pelo parâmetro da função `run(session, ..., batch_size=50)`.

2. **Estratégia de Cache em Disco (`storage/openalex/`)**:
   - Antes de efetuar requisição HTTP (`client.get`), verifica se o arquivo correspondente existe:
     - Artigo: `storage/openalex/article/{article_id}.json`
     - Pesquisador: `storage/openalex/researcher/{researcher_id}.json`
   - Se o arquivo existir e for um JSON válido, carrega o payload diretamente do disco.
   - Se o arquivo não existir ou se a leitura falhar (ex: JSON corrompido), dispara a requisição HTTP. Em caso de resposta 200, grava o arquivo em disco e segue para inserção no banco.

3. **Tratamento de Exceções e Isolamento Transacional**:
   - Cada inserção de artigo ou pesquisador é executada dentro de um savepoint (`with session.begin_nested():`). Em caso de falha de banco de dados ou erro de integridade de um registro específico, apenas o savepoint daquele registro sofre rollback, sem invalidar a transação corrente nem descartar os outros registros já inseridos no lote.
   - Falhas de rede ou requisições HTTP sem resposta válida (ex: 404 ou payload nulo) apenas incrementam `failed += 1` e não resetam o contador `uncommitted_count`, garantindo que os registros inseridos com sucesso continuem acumulando para o commit do lote e o commit residual final.
   - Sanitização de identificadores: o ORCID é normalizado com remoção de prefixos (`https://orcid.org/`, `http://orcid.org/`, `orcid.org/`) e truncado para o limite de 19 caracteres da coluna antes da inserção.
