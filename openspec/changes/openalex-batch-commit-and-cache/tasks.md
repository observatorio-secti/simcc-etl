# Tasks

## 1. Documentação e Especificação
- [x] 1.1 Criar proposta, design e tarefas OpenSpec para batch commit e cache em disco do OpenAlex.
- [x] 1.2 Atualizar `.env.example` e tabela de configuração no `README.md` com `OPENALEX_BATCH_COMMIT_SIZE`.

## 2. Configurações
- [x] 2.1 Adicionar `OPENALEX_BATCH_COMMIT_SIZE: int = 50` em `simcc_etl/core/settings.py`.

## 3. Implementação da Rotina OpenAlex
- [x] 3.1 Atualizar `enrich_from_openalex.py` para suportar leitura do cache em disco em `storage/openalex/article/` e `storage/openalex/researcher/`.
- [x] 3.2 Implementar contador e disparo de `session.commit()` a cada 50 registros com sucesso e commit residual ao final.
- [x] 3.3 Adicionar isolamento com `session.rollback()` em falhas para manter a sessão funcional.

## 4. Testes e Qualidade
- [x] 4.1 Adicionar suíte de testes unitários em `tests/unit/test_enrich_from_openalex.py` cobrindo batch commit (50 itens e lote parcial), uso de cache em disco, fallback de rede e recuperação de erro.
- [x] 4.2 Executar suíte completa de testes do `simcc-etl` garantindo 100% de sucesso.
