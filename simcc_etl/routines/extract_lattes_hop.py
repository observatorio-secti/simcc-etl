"""Execução dos workflows do Apache Hop via Hop Server (container long-lived).

O ETL não cria containers: chama a API REST do Hop Server
(`/hop/execWorkflow`), que roda o workflow de forma síncrona. Como esse
endpoint responde `OK` mesmo quando o workflow termina com erro e não devolve
o id da execução, o resultado real é obtido em `/hop/status` +
`/hop/workflowStatus` (status final e log), e a execução é removida do
servidor ao final (`/hop/removeWorkflow`).

Os XMLs do Lattes chegam ao Hop por um volume fixo: `STORAGE_DIR/xml/
hop_input` é montado em `datasets/lattes_xml` no serviço `hop`. Antes de cada
`hop_index`, o ETL preenche essa pasta com hardlinks dos XMLs selecionados.
"""

import base64
import gzip
import os
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.routines.extract_lattes_hop')

CONNECT_TIMEOUT = 10.0


class HopServerError(RuntimeError):
    """Falha de comunicação com o Hop Server ou de execução de workflow."""


def _is_fatal_log_line(line: str) -> bool:
    # Ignora 'ERROR: DUP', emitido pelo transform Unique rows sem ser falha.
    if 'ERROR: DUP' in line:
        return False
    return (
        ' - ERROR' in line
        or line.startswith('ERROR')
        or 'Workflow execution failed' in line
        or 'result=[false]' in line
    )


def _decode_log(raw: str | None) -> str:
    """Decodifica o `logging_string` do Hop (CDATA com base64 + gzip)."""
    if not raw:
        return ''
    payload = raw.strip()
    if payload.startswith('<![CDATA[') and payload.endswith(']]>'):
        payload = payload[len('<![CDATA[') : -len(']]>')]
    if not payload:
        return ''
    try:
        return gzip.decompress(base64.b64decode(payload)).decode(
            'utf-8', errors='replace'
        )
    except (ValueError, OSError):
        return payload


class HopServerClient:
    """Cliente mínimo da API REST do Hop Server."""

    def __init__(self, settings: Settings, http: httpx.Client | None = None):
        self.base_url = settings.HOP_SERVER_URL.rstrip('/') + '/hop'
        self.http = http or httpx.Client(
            auth=(settings.HOP_SERVER_USER, settings.HOP_SERVER_PASSWORD),
            timeout=httpx.Timeout(
                CONNECT_TIMEOUT, read=settings.HOP_SERVER_TIMEOUT
            ),
        )

    def close(self) -> None:
        self.http.close()

    def _get(self, path: str, params: dict | None = None) -> httpx.Response:
        url = f'{self.base_url}/{path}/'
        try:
            response = self.http.get(url, params=params)
        except httpx.TimeoutException as exc:
            raise HopServerError(
                f'Tempo esgotado aguardando o Hop Server em {url}. O '
                'workflow pode continuar rodando no servidor; aumente '
                'HOP_SERVER_TIMEOUT se a execução for legitimamente longa.'
            ) from exc
        except httpx.TransportError as exc:
            raise HopServerError(
                f'Hop Server inacessível em {self.base_url}. Suba o serviço '
                '`hop` (docker compose up -d hop) e confira HOP_SERVER_URL '
                '(http://localhost:8080 fora de container; http://hop:8080 '
                f'na rede simcc-network). Detalhe: {exc}'
            ) from exc
        if response.status_code == httpx.codes.UNAUTHORIZED:
            raise HopServerError(
                'Hop Server recusou as credenciais (HTTP 401). '
                'HOP_SERVER_USER/HOP_SERVER_PASSWORD do ETL devem ser iguais '
                'a HOP_SERVER_USER/HOP_SERVER_PASS do serviço `hop`.'
            )
        return response

    def list_workflows(self) -> list[dict]:
        response = self._get('status', {'xml': 'Y'})
        response.raise_for_status()
        root = ET.fromstring(response.content)
        return [
            {
                'name': item.findtext('workflowname', ''),
                'id': item.findtext('id', ''),
                'status': item.findtext('status_desc', ''),
            }
            for item in root.iter('workflow-status')
        ]

    def exec_workflow(self, workflow: str, variables: dict[str, str]) -> str:
        """Executa o workflow (bloqueante) e devolve a mensagem do servidor."""
        params = {'workflow': workflow, 'level': 'Basic', **variables}
        response = self._get('execWorkflow', params)
        try:
            root = ET.fromstring(response.content)
            result = root.findtext('result', '')
            message = root.findtext('message', '') or ''
        except ET.ParseError:
            result, message = '', response.text
        if response.status_code != httpx.codes.OK or result != 'OK':
            raise HopServerError(
                f'Hop Server não executou {workflow} '
                f'(HTTP {response.status_code}): {message.strip()[:2000]}'
            )
        return message

    def workflow_status(self, name: str, workflow_id: str) -> dict:
        response = self._get(
            'workflowStatus', {'name': name, 'id': workflow_id, 'xml': 'Y'}
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        return {
            'status': root.findtext('status_desc', ''),
            'error': root.findtext('error_desc', '') or '',
            'log': _decode_log(root.findtext('logging_string')),
        }

    def remove_workflow(self, name: str, workflow_id: str) -> None:
        try:
            self._get('removeWorkflow', {'name': name, 'id': workflow_id})
        except HopServerError as exc:
            logger.warning(
                'Falha ao remover execução do Hop Server',
                workflow=name,
                id=workflow_id,
                error=str(exc),
            )


def _run_hop_workflow(
    workflow_path: str,
    client: HopServerClient | None = None,
) -> Stats:
    settings = Settings()
    own_client = client is None
    client = client or HopServerClient(settings)
    workflow_name = PurePosixPath(workflow_path).stem
    variables = {
        'DATABASE_URL': settings.HOP_WRITE_JDBC_URL,
        'ADMIN_DATABASE_URL': settings.HOP_ADMIN_JDBC_URL,
    }

    try:
        # Também valida conectividade/credenciais antes da execução longa.
        known_ids = {wf['id'] for wf in client.list_workflows()}

        logger.info(
            f'Executando workflow no Hop Server: {workflow_path}',
            workflow=workflow_path,
            hop_server=client.base_url,
        )
        client.exec_workflow(f'${{PROJECT_HOME}}/{workflow_path}', variables)

        # execWorkflow não devolve o id: identifica a execução recém-criada.
        # O advisory lock do ETL garante uma execução por vez.
        new_runs = [
            wf
            for wf in client.list_workflows()
            if wf['id'] not in known_ids and wf['name'] == workflow_name
        ]
        if not new_runs:
            raise HopServerError(
                f'Execução de {workflow_path} não encontrada em /hop/status '
                'após execWorkflow; não é possível confirmar o resultado.'
            )
        run = new_runs[-1]

        try:
            status = client.workflow_status(run['name'], run['id'])
        finally:
            client.remove_workflow(run['name'], run['id'])
    finally:
        if own_client:
            client.close()

    has_fatal_error = False
    for raw_line in status['log'].splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if _is_fatal_log_line(line):
            has_fatal_error = True
            logger.error('hop.log', message=line)
        else:
            logger.info('hop.log', message=line)

    # Sucesso é exatamente 'Finished'; falhas aparecem como
    # 'Finished (with errors)' ou 'Stopped (with errors)'.
    status_desc = status['status']
    if status_desc != 'Finished' or has_fatal_error:
        raise HopServerError(
            f'Workflow do Apache Hop ({workflow_path}) falhou '
            f'(status={status_desc!r}, fatal_error={has_fatal_error}'
            f'{", erro=" + status["error"] if status["error"] else ""})'
        )

    return Stats(found=1, succeeded=1, failed=0)


def _xml_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return [
        entry
        for entry in directory.iterdir()
        if entry.is_file() and entry.name.endswith('.xml')
    ]


def _stage_hop_input(source: Path, target: Path) -> int:
    """Espelha os XMLs de `source` em `target` (pasta lida pelo Hop).

    Usa hardlinks (instantâneo, sem duplicar disco) e cai para cópia quando
    origem e destino estão em sistemas de arquivos diferentes. Esvazia apenas
    o CONTEÚDO de `target`: recriar a pasta deixaria órfão o bind mount do
    serviço `hop`.
    """
    if not source.is_dir():
        raise RuntimeError(
            f'Pasta de XMLs do Lattes não encontrada: {source}. '
            'Execute a coleta (soap_lattes) antes do hop_index ou '
            'verifique STORAGE_DIR.'
        )
    files = _xml_files(source)
    if not files:
        raise RuntimeError(
            f'Pasta de XMLs do Lattes está vazia: {source}. '
            'Execute a coleta (soap_lattes) antes do hop_index.'
        )

    target.mkdir(parents=True, exist_ok=True)
    for entry in target.iterdir():
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()

    for xml_file in files:
        destination = target / xml_file.name
        try:
            os.link(xml_file, destination)
        except OSError:
            shutil.copy2(xml_file, destination)

    logger.info(
        'XMLs preparados para o Hop Server',
        source=str(source),
        target=str(target),
        files=len(files),
    )
    return len(files)


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
    # Com filtro de IDs e run_current populada, processa só ela; senão,
    # processa o acervo completo (current).
    source = settings.xml_run_path
    if not lattes_ids or not _xml_files(source):
        source = settings.xml_current_path

    _stage_hop_input(source, settings.xml_hop_input_path)
    return _run_hop_workflow('workflows/Index.hwf')
