from unittest.mock import MagicMock

import pytest

from simcc_etl.routines.aggregate_researcher_production import (
    run as run_aggregate,
)
from simcc_etl.routines.sanitize_lattes_data import SQL_STEPS
from simcc_etl.routines.sanitize_lattes_data import run as run_sanitize


def test_sanitize_lattes_data_success():
    session = MagicMock()
    stats = run_sanitize(session)
    assert stats.found == len(SQL_STEPS)
    assert stats.succeeded == len(SQL_STEPS)
    assert stats.failed == 0
    assert session.execute.call_count == len(SQL_STEPS)
    assert session.commit.call_count == len(SQL_STEPS)


def test_sanitize_lattes_data_error():
    session = MagicMock()
    session.execute.side_effect = Exception('Database error')
    with pytest.raises(Exception, match='Database error'):
        run_sanitize(session)
    session.rollback.assert_called_once()


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
