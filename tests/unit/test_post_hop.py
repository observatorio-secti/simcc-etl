from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import polars as pl
import pytest

from simcc_etl.core.settings import Settings
from simcc_etl.routines.aggregate_researcher_production import (
    run as run_aggregate,
)
from simcc_etl.routines.sanitize_lattes_data import (
    SQL_STEPS,
    fetch_admin_metadata_parquet,
    sync_admin_data,
)
from simcc_etl.routines.sanitize_lattes_data import run as run_sanitize


@patch('simcc_etl.routines.sanitize_lattes_data.sync_admin_data')
def test_sanitize_lattes_data_success(mock_sync):
    session = MagicMock()
    stats = run_sanitize(session)
    mock_sync.assert_called_once_with(session, settings=None)
    assert stats.found == len(SQL_STEPS)
    assert stats.succeeded == len(SQL_STEPS)
    assert stats.failed == 0
    assert session.execute.call_count == len(SQL_STEPS)
    assert session.commit.call_count == len(SQL_STEPS)


@patch('simcc_etl.routines.sanitize_lattes_data.sync_admin_data')
def test_sanitize_lattes_data_error(mock_sync):
    session = MagicMock()
    session.execute.side_effect = Exception('Database error')
    with pytest.raises(Exception, match='Database error'):
        run_sanitize(session)
    session.rollback.assert_called_once()


@patch('simcc_etl.routines.sanitize_lattes_data.fetch_admin_metadata_parquet')
def test_sync_admin_data_full(mock_fetch, tmp_path):
    inst_path = tmp_path / 'institutions.parquet'
    col_path = tmp_path / 'collections.parquet'
    ri_path = tmp_path / 'researcher_institutions.parquet'
    rc_path = tmp_path / 'researcher_collections.parquet'

    pl.DataFrame({
        'name': ['Universidade Estadual de Santa Cruz'],
        'acronym': ['UESC'],
    }).write_parquet(inst_path)

    pl.DataFrame({
        'id': ['col-uuid-1'],
        'slug': ['simcc'],
        'name': ['SimCC Principal'],
        'description': ['Coleção padrão'],
        'refresh_interval_days': [1],
        'is_default': [True],
        'created_at': [datetime(2026, 1, 1)],
    }).write_parquet(col_path)

    pl.DataFrame({
        'lattes_id': ['1111222233334444'],
        'acronym': ['UESC'],
    }).write_parquet(ri_path)

    pl.DataFrame({
        'lattes_id': ['1111222233334444'],
        'collection_id': ['col-uuid-1'],
        'attributes': ['{"bolsa": "PQ-1A"}'],
        'created_at': [datetime(2026, 1, 1)],
    }).write_parquet(rc_path)

    mock_fetch.return_value = {
        'institutions': inst_path,
        'collections': col_path,
        'researcher_institutions': ri_path,
        'researcher_collections': rc_path,
    }

    write_session = MagicMock()
    m_existing_inst = MagicMock()
    m_existing_inst.mappings.return_value.all.return_value = []
    write_session.execute.return_value = m_existing_inst

    sync_admin_data(write_session)

    # Verifica que realizou commits das etapas de sincronização
    assert write_session.commit.call_count >= 3


def test_fetch_admin_metadata_parquet(tmp_path):
    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.content = b'PAR1-dummy'
    mock_resp.raise_for_status = MagicMock()

    with patch('httpx.Client.get', return_value=mock_resp) as mock_get:
        paths = fetch_admin_metadata_parquet(settings)
        assert len(paths) == 4
        assert mock_get.call_count == 4
        for p in paths.values():
            assert p.exists()
            assert p.read_bytes() == b'PAR1-dummy'


def test_fetch_admin_metadata_parquet_error(tmp_path):
    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        'Server Error', request=MagicMock(), response=MagicMock()
    )

    with patch('httpx.Client.get', return_value=mock_resp):
        with pytest.raises(RuntimeError, match='Erro ao obter metadados'):
            fetch_admin_metadata_parquet(settings)


def test_aggregate_researcher_production_all():
    session = MagicMock()
    res_mock = MagicMock()
    res_mock.fetchall.return_value = [('uuid-1',), ('uuid-2',)]
    session.execute.return_value = res_mock

    stats = run_aggregate(session)
    assert stats.found == 2
    assert stats.succeeded == 2
    assert stats.failed == 0
    assert session.execute.call_count == 2  # 1 delete + 1 insert
    session.commit.assert_called_once()


def test_aggregate_researcher_production_filtered():
    session = MagicMock()
    res_mock = MagicMock()
    res_mock.fetchall.return_value = [('uuid-1',)]
    session.execute.return_value = res_mock

    stats = run_aggregate(
        session,
        researcher_ids=['uuid-1'],
        lattes_ids=['1966167015825708'],
    )
    assert stats.found == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    assert session.execute.call_count == 2
    session.commit.assert_called_once()
