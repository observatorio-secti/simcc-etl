# Tasks

## 1. Especificação OpenSpec
- [x] 1.1 Criar `proposal.md`, `design.md` e `tasks.md` documentando o particionamento de fila e o cache de falhas 404.

## 2. Implementação no Código
- [x] 2.1 Criar estrutura de diretórios para `not_found/article/` e `not_found/researcher/` na inicialização da rotina.
- [x] 2.2 Atualizar `_get_or_fetch_payload` para registrar 404 em `not_found_file` e limpar `not_found_file` em caso de 200 OK.
- [x] 2.3 Implementar a função auxiliar `_partition_unseen_and_retry` para ordenar a fila em memória jogando os 404 conhecidos para o final.
- [x] 2.4 Integrar o particionamento aos loops de artigos e pesquisadores em `run()`.

## 3. Testes Unitários e Qualidade
- [x] 3.1 Adicionar teste unitário para `_partition_unseen_and_retry`.
- [x] 3.2 Adicionar teste unitário para persistência de 404 em `not_found` e limpeza ao obter 200 OK em retentativa.
- [x] 3.3 Adicionar teste unitário confirmando que inéditos são processados antes de retentativas.
- [x] 3.4 Validar suíte completa de testes do `simcc-etl` e linter `ruff`.

## 4. Documentação
- [x] 4.1 Atualizar `README.md` do `simcc-etl` com a descrição da priorização de fila e cache de 404.
