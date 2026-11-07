# Design: Priorização de Registros Inéditos e Fim de Fila para 404 no OpenAlex

## Contexto

O volume de publicações no SimCC ultrapassa 100 mil registros. Muitos registros contêm DOIs antigos ou digitados incorretamente no Lattes que nunca serão encontrados no OpenAlex (HTTP 404). Rodar retentativas desses itens antes de consultar os artigos válidos não testados atrasa o enriquecimento da base inteira.

## Decisões

1. **Estrutura de Armazenamento para 404 (`storage/openalex/not_found/`)**:
   - Os arquivos de respostas com sucesso (`200 OK`) continuam salvos em `storage/openalex/article/{id}.json` e `storage/openalex/researcher/{id}.json`.
   - Quando a resposta HTTP for estritamente `404 Not Found`, grava um arquivo em `storage/openalex/not_found/{entity}/{id}.json` contendo:
     ```json
     {
       "status_code": 404,
       "attempted_at": "ISO-8601 timestamp"
     }
     ```
   - Erros de rede temporários (timeouts, DNS, HTTP 5xx) **não** geram arquivo em `not_found`, permitindo retentativa imediata assim que a rede restabelecer.

2. **Particionamento de Fila em Memória ($O(1)$)**:
   - Antes de iniciar a iteração dos artigos e pesquisadores, a rotina lê os arquivos presentes em `storage/openalex/not_found/{entity}/` para um `set[str]` contendo apenas os IDs.
   - A lista de registros vinda do banco é dividida em duas listas:
     1. `unseen_rows`: registros cujo `id` não consta no conjunto de 404 conhecidos.
     2. `retry_rows`: registros cujo `id` consta no conjunto de 404 conhecidos.
   - A lista final a ser processada é a concatenação `unseen_rows + retry_rows`.
   - Isso garante que todos os itens novos sejam processados prioritariamente.

3. **Política de Execução da Fila Secundária (Opção B - Retentativa Direta)**:
   - Quando todos os itens inéditos forem finalizados, o loop avança para os itens de `retry_rows`.
   - Eles executam uma nova chamada à API. Se a resposta continuar 404, o arquivo em `not_found` tem seu timestamp atualizado. Se a resposta for 200 OK, os dados são salvos em `article/{id}.json`, o arquivo correspondente em `not_found/` é removido e o registro é inserido no banco de dados.

4. **Isolamento de Impacto no Banco**:
   - Não há necessidade de alterações de schema, novas tabelas ou migrações Alembic no PostgreSQL. O estado de "já testado com 404" é mantido no sistema de arquivos local do ETL.
