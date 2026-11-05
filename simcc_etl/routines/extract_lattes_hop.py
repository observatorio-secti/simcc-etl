import os
from pathlib import Path

import docker
from docker.errors import DockerException
from requests.exceptions import RequestException
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.extract_lattes_hop')

HOP_XML_BIND = '/files/jade-extrator/datasets/lattes_xml'


def _docker_client() -> docker.DockerClient:
    """Cria o cliente Docker com mensagem de erro acionável em pt-BR."""
    try:
        client = docker.from_env()
        client.ping()
    except (DockerException, RequestException, OSError) as exc:
        raise RuntimeError(
            'Não foi possível acessar o daemon Docker para disparar o '
            'Apache Hop. Se o ETL roda em container, monte o socket '
            '(/var/run/docker.sock:/var/run/docker.sock) e adicione o '
            'GID do grupo docker do host ao serviço (group_add: '
            '["${DOCKER_GID}"], obtido com `getent group docker | cut -d: '
            '-f3`). Fora de container, verifique se o Docker está ativo '
            f'e se o usuário pertence ao grupo docker. Detalhe: {exc}'
        ) from exc
    return client


def _ensure_xml_dir(xml_dir: Path) -> None:
    """Falha se a pasta de XMLs não existir ou estiver vazia."""
    if not xml_dir.is_dir():
        raise RuntimeError(
            f'Pasta de XMLs do Lattes não encontrada: {xml_dir}. '
            'Execute a coleta (soap_lattes) antes do hop_index ou '
            'verifique STORAGE_DIR.'
        )
    if not any(xml_dir.iterdir()):
        raise RuntimeError(
            f'Pasta de XMLs do Lattes está vazia: {xml_dir}. '
            'Execute a coleta (soap_lattes) antes do hop_index.'
        )


def _host_path(local_path: Path, settings: Settings) -> str:
    """Traduz um caminho visto pelo ETL para o caminho no host do Docker.

    O bind mount é resolvido pelo daemon Docker (no host), não pelo processo
    do ETL. Sem HOST_STORAGE_DIR, assume-se que o ETL roda no próprio host e
    o caminho local é usado diretamente.
    """
    if settings.HOST_STORAGE_DIR is None:
        return str(local_path.resolve())

    host_root = settings.HOST_STORAGE_DIR
    if not host_root.is_absolute():
        raise RuntimeError(
            f'HOST_STORAGE_DIR deve ser um caminho absoluto no host '
            f'(recebido: {host_root}).'
        )

    # abspath (sem seguir symlinks): o host não enxerga links internos do
    # container, apenas a mesma árvore montada em STORAGE_DIR.
    storage_root = Path(os.path.abspath(settings.STORAGE_DIR))
    absolute = Path(os.path.abspath(local_path))
    try:
        relative = absolute.relative_to(storage_root)
    except ValueError as exc:
        raise RuntimeError(
            f'O caminho {absolute} está fora de STORAGE_DIR '
            f'({storage_root}); não é possível traduzi-lo com '
            'HOST_STORAGE_DIR.'
        ) from exc
    return str(host_root / relative)


def _run_hop_workflow(
    workflow_path: str,
    xml_mount_path: Path | None = None,
) -> Stats:
    settings = Settings()

    volumes = None
    if xml_mount_path is not None:
        _ensure_xml_dir(xml_mount_path)
        volumes = {
            _host_path(xml_mount_path, settings): {
                'bind': HOP_XML_BIND,
                'mode': 'rw',
            }
        }

    client = _docker_client()

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

    logger.info(
        f'Disparando container Apache Hop para workflow: {workflow_path}',
        workflow=workflow_path,
        image=settings.HOP_IMAGE,
        volumes=volumes,
    )

    container = client.containers.run(
        image=settings.HOP_IMAGE,
        environment=environment,
        volumes=volumes,
        network=settings.DOCKER_NETWORK,
        detach=True,
    )

    has_fatal_error = False
    try:
        for raw_line in container.logs(stream=True, follow=True):
            line = raw_line.decode('utf-8', errors='replace').strip()
            if not line:
                continue

            # Detecta erros críticos no log do Hop (ignorando 'ERROR: DUP'
            # do transform Unique rows)
            is_dup_error = 'ERROR: DUP' in line
            if not is_dup_error and (
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

    # _run_hop_workflow valida a pasta (existe e não vazia) e traduz o
    # caminho para o host via HOST_STORAGE_DIR quando definido.
    return _run_hop_workflow('workflows/Index.hwf', xml_mount_path=mount_dir)
