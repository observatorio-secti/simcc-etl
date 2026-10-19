from unittest.mock import MagicMock

import pytest

from simcc_etl.pipeline import (
    AlreadyRunningError,
    Stats,
    get_available_routines,
    get_default_routines,
    register_routine,
    release_lock,
    try_acquire_lock,
)


def test_stats_dataclass():
    s = Stats(found=10, succeeded=9, failed=1)
    assert s.found == 10
    assert s.succeeded == 9
    assert s.failed == 1


def test_register_and_get_routines():
    def dummy_fn(session, lattes_ids=None):
        return Stats(found=1, succeeded=1, failed=0)

    register_routine('dummy_routine', dummy_fn)
    assert 'dummy_routine' in get_available_routines()


def test_default_routines_excludes_hop_seed():
    default_routines = get_default_routines()
    assert 'hop_seed' not in default_routines
    assert 'soap_lattes' in default_routines
    assert 'hop_index' in default_routines
    assert 'publish_to_read_db' in default_routines


def test_try_acquire_lock_mock():
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = True

    assert try_acquire_lock(mock_conn) is True

    mock_conn.execute.return_value.scalar.return_value = False
    assert try_acquire_lock(mock_conn) is False


def test_release_lock_mock():
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = True

    assert release_lock(mock_conn) is True


def test_already_running_error():
    with pytest.raises(AlreadyRunningError):
        raise AlreadyRunningError('ETL já em execução')
