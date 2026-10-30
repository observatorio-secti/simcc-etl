# Tasks

## 1. Configuração e Settings
- [x] 1.1 Adicionar `SOAP_MAX_WORKERS: int = 16` em `simcc_etl/core/settings.py`.

## 2. Rotina soap_lattes com TTL e Paralelismo
- [x] 2.1 Atualizar `simcc_etl/routines/soap_lattes.py` para receber mapa de `{lattes_id: refresh_interval_days}` a partir do Parquet.
- [x] 2.2 Implementar verificação de idade do arquivo XML contra `refresh_interval_days`.
- [x] 2.3 Paralelizar download usando `ThreadPoolExecutor(max_workers=settings.SOAP_MAX_WORKERS)`.

## 3. Sincronização Direta de Coleções e Vínculos
- [x] 3.1 Atualizar `simcc_etl/routines/sanitize_lattes_data.py` com funções de sync do `admin_db` para `write_db`: instituições, vínculos institucionais (`researcher_institution`), coleções (`collection`) e vínculos de coleções (`researcher_collection`).
- [x] 3.2 Remover a query legada `INSERT INTO researcher_institution ... SELECT id, institution_id FROM researcher`.

## 4. Pipeline Hop
- [x] 4.1 Limpar `ImportResearcher.hpl` no `Jade-Extrator-Hop` removendo `institution_id`, `status` e `extra_field`.
