from unittest.mock import MagicMock, patch

import pytest

from simcc_etl.routines.aggregate_researcher_production import (
    run as run_aggregate,
)
from simcc_etl.routines.sanitize_lattes_data import (
    SQL_STEPS,
    sync_admin_data,
)
from simcc_etl.routines.sanitize_lattes_data import run as run_sanitize


@patch('simcc_etl.routines.sanitize_lattes_data.sync_admin_data')
def test_sanitize_lattes_data_success(mock_sync):
    session = MagicMock()
    stats = run_sanitize(session)
    mock_sync.assert_called_once_with(session, admin_engine=None)
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


def test_sync_admin_data_full():
    admin_engine = MagicMock()
    admin_conn = MagicMock()
    admin_engine.connect.return_value.__enter__.return_value = admin_conn

    # Mock admin_conn executes:
    # 1. academic.institutions
    # 2. academic.collections
    # 3. academic.researcher_institutions
    # 4. academic.researcher_collections
    m_res = MagicMock()
    m_res.mappings.return_value.all.return_value = [
        {'id': 'res-uuid-1', 'name': 'Pesquisador Teste', 'lattes_id': '1111222233334444'}
    ]

    m_inst = MagicMock()
    m_inst.mappings.return_value.all.return_value = [
        {'name': 'Universidade Estadual de Santa Cruz', 'acronym': 'UESC'}
    ]

    m_col = MagicMock()
    m_col.mappings.return_value.all.return_value = [
        {
            'id': 'col-uuid-1',
            'slug': 'simcc',
            'name': 'SimCC Principal',
            'description': 'Coleção padrão',
            'refresh_interval_days': 1,
            'is_default': True,
            'created_at': '2026-01-01T00:00:00Z',
        }
    ]

    m_ri = MagicMock()
    m_ri.mappings.return_value.all.return_value = [
        {'lattes_id': '1111222233334444', 'acronym': 'UESC'}
    ]

    m_rc = MagicMock()
    m_rc.mappings.return_value.all.return_value = [
        {
            'lattes_id': '1111222233334444',
            'collection_id': 'col-uuid-1',
            'attributes': {'bolsa': 'PQ-1A'},
            'created_at': '2026-01-01T00:00:00Z',
        }
    ]

    admin_conn.execute.side_effect = [m_res, m_inst, m_col, m_ri, m_rc]

    write_session = MagicMock()
    m_existing_inst = MagicMock()
    m_existing_inst.mappings.return_value.all.return_value = []
    write_session.execute.return_value = m_existing_inst

    sync_admin_data(write_session, admin_engine=admin_engine)

    # Verifica que realizou commits das etapas de sincronização
    assert write_session.commit.call_count >= 4


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
