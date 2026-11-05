"""Testes do disparo do Apache Hop (bind mount dos XMLs e acesso ao Docker).

O cliente Docker é sempre mockado: nenhum container real é criado.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from docker.errors import DockerException

from simcc_etl.core.settings import Settings
from simcc_etl.routines import extract_lattes_hop as hop

HOP_BIND = '/files/jade-extrator/datasets/lattes_xml'


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """STORAGE_DIR temporário com a pasta xml/current populada."""
    storage_dir = tmp_path / 'storage'
    current = storage_dir / 'xml' / 'current'
    current.mkdir(parents=True)
    (current / '0000000000000001.xml').write_text('<CURRICULO-VITAE/>')
    monkeypatch.setenv('STORAGE_DIR', str(storage_dir))
    monkeypatch.delenv('HOST_STORAGE_DIR', raising=False)
    return storage_dir


@pytest.fixture
def docker_client():
    """Cliente Docker falso cujo container termina com sucesso."""
    client = MagicMock()
    container = MagicMock()
    container.logs.return_value = [b'Workflow finished']
    container.wait.return_value = {'StatusCode': 0}
    client.containers.run.return_value = container
    with patch.object(hop.docker, 'from_env', return_value=client):
        yield client


def _volumes(client):
    return client.containers.run.call_args.kwargs['volumes']


def test_index_mounts_local_path_without_host_storage_dir(
    storage, docker_client
):
    stats = hop.run_index(MagicMock())

    assert stats.succeeded == 1
    expected = str((storage / 'xml' / 'current').resolve())
    assert _volumes(docker_client) == {
        expected: {'bind': HOP_BIND, 'mode': 'rw'}
    }


def test_index_translates_path_with_host_storage_dir(
    storage, docker_client, monkeypatch
):
    monkeypatch.setenv('HOST_STORAGE_DIR', '/opt/simcc/storage')

    hop.run_index(MagicMock())

    assert _volumes(docker_client) == {
        '/opt/simcc/storage/xml/current': {'bind': HOP_BIND, 'mode': 'rw'}
    }


def test_index_with_lattes_ids_translates_run_current(
    storage, docker_client, monkeypatch
):
    run_current = storage / 'xml' / 'run_current'
    run_current.mkdir(parents=True)
    (run_current / '0000000000000002.xml').write_text('<CURRICULO-VITAE/>')
    monkeypatch.setenv('HOST_STORAGE_DIR', '/opt/simcc/storage')

    hop.run_index(MagicMock(), lattes_ids=['0000000000000002'])

    assert list(_volumes(docker_client)) == [
        '/opt/simcc/storage/xml/run_current'
    ]


def test_index_with_lattes_ids_falls_back_to_current_when_run_empty(
    storage, docker_client, monkeypatch
):
    (storage / 'xml' / 'run_current').mkdir(parents=True)
    monkeypatch.setenv('HOST_STORAGE_DIR', '/opt/simcc/storage')

    hop.run_index(MagicMock(), lattes_ids=['0000000000000002'])

    assert list(_volumes(docker_client)) == ['/opt/simcc/storage/xml/current']


def test_index_fails_when_xml_dir_missing(tmp_path, monkeypatch):
    monkeypatch.setenv('STORAGE_DIR', str(tmp_path / 'inexistente'))

    with patch.object(hop.docker, 'from_env') as from_env:
        with pytest.raises(RuntimeError, match='não encontrada'):
            hop.run_index(MagicMock())
        from_env.assert_not_called()


def test_index_fails_when_xml_dir_empty(tmp_path, monkeypatch):
    (tmp_path / 'xml' / 'current').mkdir(parents=True)
    monkeypatch.setenv('STORAGE_DIR', str(tmp_path))

    with patch.object(hop.docker, 'from_env') as from_env:
        with pytest.raises(RuntimeError, match='vazia'):
            hop.run_index(MagicMock())
        from_env.assert_not_called()


def test_relative_host_storage_dir_is_rejected(storage, monkeypatch):
    monkeypatch.setenv('HOST_STORAGE_DIR', 'relativo/storage')

    with pytest.raises(RuntimeError, match='absoluto'):
        hop._host_path(storage / 'xml' / 'current', Settings())


def test_path_outside_storage_dir_is_rejected(storage, tmp_path, monkeypatch):
    monkeypatch.setenv('HOST_STORAGE_DIR', '/opt/simcc/storage')

    with pytest.raises(RuntimeError, match='fora de STORAGE_DIR'):
        hop._host_path(tmp_path / 'outro', Settings())


def test_empty_host_storage_dir_env_means_none(monkeypatch):
    monkeypatch.setenv('HOST_STORAGE_DIR', '')

    assert Settings().HOST_STORAGE_DIR is None


def test_seed_runs_without_volume(storage, docker_client):
    session = MagicMock()
    session.execute.return_value.scalar.return_value = False

    stats = hop.run_seed(session)

    assert stats.succeeded == 1
    kwargs = docker_client.containers.run.call_args.kwargs
    assert kwargs['volumes'] is None
    assert kwargs['environment']['HOP_FILE_PATH'].endswith(
        'workflows/Seeding.hwf'
    )


def test_seed_runs_even_without_xml_dir(tmp_path, monkeypatch, docker_client):
    monkeypatch.setenv('STORAGE_DIR', str(tmp_path / 'inexistente'))
    session = MagicMock()
    session.execute.return_value.scalar.return_value = False

    stats = hop.run_seed(session)

    assert stats.succeeded == 1


@pytest.mark.parametrize(
    'error',
    [
        DockerException('Error while fetching server API version'),
        PermissionError(13, 'Permission denied'),
    ],
)
def test_friendly_error_when_docker_unreachable(storage, error):
    with patch.object(hop.docker, 'from_env', side_effect=error):
        with pytest.raises(RuntimeError) as exc_info:
            hop.run_index(MagicMock())

    message = str(exc_info.value)
    assert '/var/run/docker.sock' in message
    assert 'group_add' in message
    assert 'DOCKER_GID' in message


def test_friendly_error_when_ping_fails(storage):
    client = MagicMock()
    client.ping.side_effect = DockerException('connection refused')

    with patch.object(hop.docker, 'from_env', return_value=client):
        with pytest.raises(RuntimeError, match='daemon Docker'):
            hop.run_index(MagicMock())
    client.containers.run.assert_not_called()


def test_hop_failure_still_raises(storage, docker_client):
    container = docker_client.containers.run.return_value
    container.wait.return_value = {'StatusCode': 1}

    with pytest.raises(RuntimeError, match='falhou'):
        hop.run_index(MagicMock())
    container.remove.assert_called_once_with(force=True)


def test_relative_storage_dir_is_resolved_against_cwd(
    tmp_path, monkeypatch, docker_client
):
    """Simula o container: WORKDIR /app e STORAGE_DIR relativo."""
    current = tmp_path / 'storage' / 'xml' / 'current'
    current.mkdir(parents=True)
    (current / 'a.xml').write_text('<x/>')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('STORAGE_DIR', 'storage')
    monkeypatch.setenv('HOST_STORAGE_DIR', '/opt/simcc/storage')

    hop.run_index(MagicMock())

    assert list(_volumes(docker_client)) == [
        str(Path('/opt/simcc/storage/xml/current'))
    ]
