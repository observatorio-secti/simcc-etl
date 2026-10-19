from pathlib import Path

import docker
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.extract_lattes_hop')


def _run_hop_workflow(
    workflow_path: str,
    xml_mount_path: Path | None = None,
) -> Stats:
    settings = Settings()
    client = docker.from_env()

    environment = {
        'HOP_RUN_CONFIG': 'local',
        'HOP_LOG_LEVEL': 'Basic',
        'HOP_PROJECT_NAME': 'Jade-Extrator',
        'HOP_PROJECT_FOLDER': '/files/jade-extrator',
        'HOP_FILE_PATH': f'/files/jade-extrator/{workflow_path}',
        'HOP_RUN_PARAMETERS': (
            f'DATABASE_URL={settings.HOP_WRITE_JDBC_URL},'
            f'ADMIN_DATABASE_URL={settings.HOP_ADMIN_JDBC_URL}'
        ),
        'DATABASE_URL': settings.HOP_WRITE_JDBC_URL,
        'ADMIN_DATABASE_URL': settings.HOP_ADMIN_JDBC_URL,
    }

    volumes = {}
    if xml_mount_path and xml_mount_path.exists():
        volumes[str(xml_mount_path.resolve())] = {
            'bind': '/files/jade-extrator/datasets/lattes_xml',
            'mode': 'rw',
        }

    logger.info(
        f'Disparando container Apache Hop para workflow: {workflow_path}',
        workflow=workflow_path,
        image=settings.HOP_IMAGE,
    )

    container = client.containers.run(
        image=settings.HOP_IMAGE,
        environment=environment,
        volumes=volumes if volumes else None,
        network=settings.DOCKER_NETWORK,
        detach=True,
    )

    has_fatal_error = False
    try:
        for raw_line in container.logs(stream=True, follow=True):
            line = raw_line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            # Detecta erros críticos no log do Hop
            if (
                ' - ERROR' in line
                or line.startswith('ERROR')
                or 'Workflow execution failed' in line
                or 'result=[false]' in line
            ):
                has_fatal_error = True
                logger.error('hop.log', message=line)
            else:
                logger.info('hop.log', message=line)

        result = container.wait()
        exit_code = result.get('StatusCode', 1)

        if exit_code != 0 or has_fatal_error:
            raise RuntimeError(
                f'Workflow do Apache Hop ({workflow_path}) falhou '
                f'(exit_code={exit_code}, fatal_error={has_fatal_error})'
            )

        return Stats(found=1, succeeded=1, failed=0)
    finally:
        try:
            container.remove(force=True)
        except Exception:
            pass


def is_database_seeded(session: Session) -> bool:
    """Verifica se o banco já possui os dados carregados pelo Seed."""
    try:
        sql = text('SELECT EXISTS (SELECT 1 FROM country LIMIT 1)')
        return bool(session.execute(sql).scalar())
    except Exception as e:
        logger.warning(
            f'Falha ao verificar seed no banco: {e}. Prosseguindo com Seed.'
        )
        return False


def run_seed(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
    force: bool = False,
) -> Stats:
    """Executa o workflow Seeding.hwf do Hop de forma idempotente.

    O workflow Seeding.hwf insere dados (países, estados, JCR, Qualis) usando
    TableOutput sem truncate e sem cláusula ON CONFLICT. Executá-lo mais de uma
    vez em um banco já inicializado resulta em erro de chave duplicada
    (duplicate key value violates unique constraint).

    Se o banco já possuir registros na tabela 'country', a rotina pula a
    execução e retorna sucesso sem disparar o container Apache Hop.
    """
    if not force and is_database_seeded(session):
        logger.info(
            'Banco de dados já contém registros de semente '
            '(países/estados/JCR/Qualis). Pulando execução de Seeding.hwf.'
        )
        return Stats(found=0, succeeded=0, failed=0)

    return _run_hop_workflow('workflows/Seeding.hwf')


def run_index(
    session: Session, *, lattes_ids: list[str] | None = None
) -> Stats:
    """Executa o workflow principal de extração de XMLs (Index.hwf)."""
    settings = Settings()
    # Se houver filtro de IDs e a pasta run_current tiver arquivos, monta ela
    mount_dir = settings.xml_run_path
    if (
        not lattes_ids
        or not mount_dir.exists()
        or not any(mount_dir.iterdir())
    ):
        mount_dir = settings.xml_current_path

    return _run_hop_workflow('workflows/Index.hwf', xml_mount_path=mount_dir)
