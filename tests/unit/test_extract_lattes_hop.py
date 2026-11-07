"""Testes da execução do Apache Hop via Hop Server (long-lived).

O Hop Server é simulado com `httpx.MockTransport`, reproduzindo o
comportamento observado no Hop 2.19: `execWorkflow` responde `OK` mesmo
quando o workflow falha e não devolve o id da execução; o resultado real vem
de `/hop/status` + `/hop/workflowStatus`.
"""

import base64
import gzip
import os
import uuid
from unittest.mock import MagicMock

import httpx
import pytest

from simcc_etl.core.settings import Settings
from simcc_etl.routines import extract_lattes_hop as hop


def _encode_log(text: str) -> str:
    payload = base64.b64encode(gzip.compress(text.encode())).decode()
    return f'&lt;![CDATA[{payload}]]&gt;'


class FakeHopServer:
    """Hop Server em memória com os endpoints usados pelo ETL."""

    def __init__(self):
        self.runs: dict[str, dict] = {}
        self.requests: list[httpx.Request] = []
        self.removed: list[str] = []
        self.next_status = 'Finished'
        self.next_log = 'Index - Workflow execution finished\n'
        self.exec_response = None
        self.down = False
        self.unauthorized = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.down:
            raise httpx.ConnectError('Connection refused', request=request)
        if self.unauthorized:
            return httpx.Response(401, text='Unauthorized')

        path = request.url.path.rstrip('/')
        params = request.url.params
        if path == '/hop/status':
            items = ''.join(
                f'<workflow-status><workflowname>{r["name"]}</workflowname>'
                f'<id>{rid}</id><status_desc>{r["status"]}</status_desc>'
                '</workflow-status>'
                for rid, r in self.runs.items()
            )
            return httpx.Response(
                200,
                text=(
                    '<serverstatus><workflow_status_list>'
                    f'{items}</workflow_status_list></serverstatus>'
                ),
            )
        if path == '/hop/execWorkflow':
            if self.exec_response is not None:
                return self.exec_response
            name = params['workflow'].rsplit('/', 1)[-1].removesuffix('.hwf')
            self.runs[str(uuid.uuid4())] = {
                'name': name,
                'status': self.next_status,
                'log': self.next_log,
            }
            return httpx.Response(
                200,
                text=(
                    '<webresult><result>OK</result><message>Workflow '
                    'executed successfully</message><id/></webresult>'
                ),
            )
        if path == '/hop/workflowStatus':
            run = self.runs[params['id']]
            return httpx.Response(
                200,
                text=(
                    '<workflow-status>'
                    f'<workflowname>{run["name"]}</workflowname>'
                    f'<status_desc>{run["status"]}</status_desc>'
                    '<error_desc/>'
                    f'<logging_string>{_encode_log(run["log"])}'
                    '</logging_string></workflow-status>'
                ),
            )
        if path == '/hop/removeWorkflow':
            self.removed.append(params['id'])
            self.runs.pop(params['id'], None)
            return httpx.Response(
                200, text='<webresult><result>OK</result></webresult>'
            )
        return httpx.Response(404)

    def exec_params(self) -> httpx.QueryParams:
        (request,) = [r for r in self.requests if 'execWorkflow' in r.url.path]
        return request.url.params


@pytest.fixture
def hop_server(monkeypatch):
    server = FakeHopServer()
    real_client = hop.HopServerClient

    def factory(settings):
        return real_client(
            settings,
            http=httpx.Client(transport=httpx.MockTransport(server.handler)),
        )

    monkeypatch.setattr(hop, 'HopServerClient', factory)
    return server


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """STORAGE_DIR temporário com XMLs em xml/current."""
    storage_dir = tmp_path / 'storage'
    current = storage_dir / 'xml' / 'current'
    current.mkdir(parents=True)
    for lattes_id in ('0000000000000001', '0000000000000002'):
        (current / f'{lattes_id}.xml').write_text('<CURRICULO-VITAE/>')
    monkeypatch.setenv('STORAGE_DIR', str(storage_dir))
    return storage_dir


def _hop_input(storage_dir):
    return storage_dir / 'xml' / 'hop_input'


def _staged(storage_dir):
    return sorted(p.name for p in _hop_input(storage_dir).iterdir())


# --- hop_index: preparação dos XMLs -------------------------------------


def test_index_stages_current_as_hardlinks_and_succeeds(storage, hop_server):
    stats = hop.run_index(MagicMock())

    assert stats.succeeded == 1
    assert _staged(storage) == ['0000000000000001.xml', '0000000000000002.xml']
    source = storage / 'xml' / 'current' / '0000000000000001.xml'
    staged = _hop_input(storage) / '0000000000000001.xml'
    assert os.path.samefile(source, staged)


def test_index_with_lattes_ids_uses_run_current(storage, hop_server):
    run_current = storage / 'xml' / 'run_current'
    run_current.mkdir()
    (run_current / '0000000000000009.xml').write_text('<CURRICULO-VITAE/>')

    hop.run_index(MagicMock(), lattes_ids=['0000000000000009'])

    assert _staged(storage) == ['0000000000000009.xml']


def test_index_with_lattes_ids_falls_back_to_current(storage, hop_server):
    (storage / 'xml' / 'run_current').mkdir()

    hop.run_index(MagicMock(), lattes_ids=['0000000000000009'])

    assert len(_staged(storage)) == 2


def test_index_replaces_previous_content_but_keeps_folder(storage, hop_server):
    target = _hop_input(storage)
    target.mkdir(parents=True)
    (target / 'antigo.xml').write_text('<x/>')
    (target / 'sub').mkdir()
    inode = target.stat().st_ino

    hop.run_index(MagicMock())

    assert 'antigo.xml' not in _staged(storage)
    assert 'sub' not in _staged(storage)
    # Recriar a pasta deixaria órfão o bind mount do serviço `hop`.
    assert target.stat().st_ino == inode


def test_index_falls_back_to_copy_when_hardlink_fails(
    storage, hop_server, monkeypatch
):
    def fail_link(*_args, **_kwargs):
        raise OSError(18, 'Invalid cross-device link')

    monkeypatch.setattr(hop.os, 'link', fail_link)

    hop.run_index(MagicMock())

    staged = _hop_input(storage) / '0000000000000001.xml'
    assert staged.read_text() == '<CURRICULO-VITAE/>'


def test_index_fails_when_xml_dir_missing(tmp_path, monkeypatch, hop_server):
    monkeypatch.setenv('STORAGE_DIR', str(tmp_path / 'inexistente'))

    with pytest.raises(RuntimeError, match='não encontrada'):
        hop.run_index(MagicMock())
    assert hop_server.requests == []


def test_index_fails_when_xml_dir_has_no_xml(
    tmp_path, monkeypatch, hop_server
):
    current = tmp_path / 'xml' / 'current'
    current.mkdir(parents=True)
    (current / 'leia-me.txt').write_text('sem xml')
    monkeypatch.setenv('STORAGE_DIR', str(tmp_path))

    with pytest.raises(RuntimeError, match='vazia'):
        hop.run_index(MagicMock())
    assert hop_server.requests == []


# --- Execução no Hop Server ----------------------------------------------


def test_exec_sends_project_path_and_jdbc_variables(
    storage, hop_server, monkeypatch
):
    monkeypatch.setenv('HOP_WRITE_JDBC_URL', 'jdbc:postgresql://w/db')
    monkeypatch.setenv('HOP_ADMIN_JDBC_URL', 'jdbc:postgresql://a/db')

    hop.run_index(MagicMock())

    params = hop_server.exec_params()
    assert params['workflow'] == '${PROJECT_HOME}/workflows/Index.hwf'
    assert params['DATABASE_URL'] == 'jdbc:postgresql://w/db'
    assert params['ADMIN_DATABASE_URL'] == 'jdbc:postgresql://a/db'


def test_exec_removes_run_from_server_after_success(storage, hop_server):
    hop.run_index(MagicMock())

    assert len(hop_server.removed) == 1
    assert hop_server.runs == {}


def test_failure_detected_from_status_even_when_exec_says_ok(
    storage, hop_server
):
    hop_server.next_status = 'Stopped (with errors)'

    with pytest.raises(hop.HopServerError, match='Stopped'):
        hop.run_index(MagicMock())
    assert len(hop_server.removed) == 1


def test_failure_detected_from_log_lines(storage, hop_server):
    hop_server.next_log = (
        'Index - Starting action [X]\n'
        'Index - Finished action [X] (result=[false])\n'
    )

    with pytest.raises(hop.HopServerError, match='fatal_error=True'):
        hop.run_index(MagicMock())


def test_dup_error_lines_are_not_fatal(storage, hop_server):
    hop_server.next_log = 'Unique rows - ERROR: DUP 123\n'

    assert hop.run_index(MagicMock()).succeeded == 1


def test_exec_error_response_is_raised(storage, hop_server):
    hop_server.exec_response = httpx.Response(
        500,
        text=(
            '<webresult><result>ERROR</result>'
            '<message>Unable to find workflow</message><id/></webresult>'
        ),
    )

    with pytest.raises(hop.HopServerError, match='Unable to find workflow'):
        hop.run_index(MagicMock())


def test_missing_run_in_status_is_an_error(storage, hop_server):
    hop_server.exec_response = httpx.Response(
        200, text='<webresult><result>OK</result><id/></webresult>'
    )

    with pytest.raises(hop.HopServerError, match='não encontrada'):
        hop.run_index(MagicMock())


def test_friendly_error_when_server_unreachable(storage, hop_server):
    hop_server.down = True

    with pytest.raises(hop.HopServerError) as exc_info:
        hop.run_index(MagicMock())

    message = str(exc_info.value)
    assert 'inacessível' in message
    assert 'HOP_SERVER_URL' in message
    assert 'docker compose up -d hop' in message


def test_friendly_error_on_bad_credentials(storage, hop_server):
    hop_server.unauthorized = True

    with pytest.raises(hop.HopServerError, match='HOP_SERVER_PASS'):
        hop.run_index(MagicMock())


def test_timeout_is_reported(storage, monkeypatch):
    def timeout(request):
        raise httpx.ReadTimeout('timed out', request=request)

    client = hop.HopServerClient(
        Settings(), http=httpx.Client(transport=httpx.MockTransport(timeout))
    )

    with pytest.raises(hop.HopServerError, match='HOP_SERVER_TIMEOUT'):
        hop._run_hop_workflow('workflows/Index.hwf', client=client)


# --- hop_seed --------------------------------------------------------------


def test_seed_runs_without_xml_dir(tmp_path, monkeypatch, hop_server):
    monkeypatch.setenv('STORAGE_DIR', str(tmp_path / 'inexistente'))
    session = MagicMock()
    session.execute.return_value.scalar.return_value = False

    stats = hop.run_seed(session)

    assert stats.succeeded == 1
    params = hop_server.exec_params()
    assert params['workflow'] == '${PROJECT_HOME}/workflows/Seeding.hwf'
    assert not (tmp_path / 'inexistente').exists()


# --- Utilitários -----------------------------------------------------------


def test_decode_log_handles_plain_and_empty_values():
    assert hop._decode_log(None) == ''
    assert hop._decode_log('<![CDATA[]]>') == ''
    payload = base64.b64encode(gzip.compress(b'linha')).decode()
    assert hop._decode_log(f'<![CDATA[{payload}]]>') == 'linha'


def test_settings_hop_server_defaults(monkeypatch):
    for var in ('HOP_SERVER_URL', 'HOP_SERVER_USER', 'HOP_SERVER_PASSWORD'):
        monkeypatch.delenv(var, raising=False)
    settings = Settings(_env_file=None)

    assert settings.HOP_SERVER_URL == 'http://localhost:8080'
    assert settings.xml_hop_input_path.parts[-2:] == ('xml', 'hop_input')
