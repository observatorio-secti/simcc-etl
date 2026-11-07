import argparse
import os
import sys
from pathlib import Path

from simcc_etl.core.logging import configure_logging
from simcc_etl.pipeline import (
    AlreadyRunningError,
    get_available_routines,
    run_pipeline,
)


def main():
    os.umask(0o022)
    os.chdir(Path(__file__).resolve().parents[1])
    parser = argparse.ArgumentParser(description='SimCC ETL CLI')
    parser.add_argument(
        '--routines',
        nargs='*',
        default=None,
        help='Nomes das rotinas a executar na ordem.',
    )
    parser.add_argument(
        '--lattes-ids',
        nargs='*',
        default=None,
        help='Lista de Lattes IDs para execução parcial.',
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='Lista as rotinas registradas e encerra.',
    )

    args = parser.parse_args()

    if args.list:
        print('Rotinas disponíveis no catálogo:')
        for r in get_available_routines():
            print(f'  - {r}')
        sys.exit(0)

    configure_logging()

    try:
        run_pipeline(
            routines=args.routines,
            lattes_ids=args.lattes_ids,
            triggered_by='cli',
        )
        print('✅ ETL finalizado com sucesso.')
    except AlreadyRunningError:
        print('❌ Erro: Outro processo ETL já está em execução no momento.')
        sys.exit(1)
    except Exception as e:
        print(f'❌ Erro durante a execução do ETL: {str(e)}')
        sys.exit(1)


if __name__ == '__main__':
    main()
