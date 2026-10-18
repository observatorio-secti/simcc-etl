# SimCC ETL

Serviço de extração, transformação e carga (ETL) desacoplado do SIMCC.

## Estrutura do Projeto

- `simcc_etl/`: Código-fonte da aplicação (FastAPI, CLI, rotinas e orquestrador).
- `simcc_etl/core/`: Configurações, conexões de banco de dados e utilitários.
- `simcc_etl/routines/`: Módulos de rotinas individuais.
- `storage/`: Diretório de arquivos de trabalho (XMLs Lattes, CSVs de seed).
- `logs/`: Logs estruturados das execuções em formato JSONL.
- `tests/`: Testes unitários e de integração.

## Comandos Úteis (via taskipy)

- `poetry run task lint`: Executa verificação estática com Ruff.
- `poetry run task format`: Corrige e formata código com Ruff.
- `poetry run task test`: Executa suíte de testes com cobertura.
- `poetry run task run`: Inicia a API REST em modo desenvolvimento.
