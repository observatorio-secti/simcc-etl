import io
from unittest.mock import MagicMock, patch

import httpx
import polars as pl
import pytest

from simcc_etl.core.settings import Settings
from simcc_etl.routines.remove_researchers import run as run_remove_researchers


def _parquet_bytes(lattes_ids: list[str]) -> bytes:
    df = pl.DataFrame(
        {'lattes_id': lattes_ids},
        schema={'lattes_id': pl.Utf8},
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def test_remove_researchers_by_admin_diff(tmp_path):
    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.content = _parquet_bytes(['1111222233334444'])
    mock_resp.raise_for_status = MagicMock()

    session = MagicMock()
    select_res = MagicMock()
    select_res.fetchall.return_value = [('uuid-removed-1',), ('uuid-removed-2',)]
    session.execute.side_effect = [
        select_res,
        MagicMock(),
        MagicMock(),
        MagicMock(),
    ]

    with patch('httpx.Client.get', return_value=mock_resp):
        stats = run_remove_researchers(session, settings=settings)

    assert stats.found == 2
    assert stats.succeeded == 2
    assert stats.failed == 0

    sql_calls = [str(c.args[0]) for c in session.execute.call_args_list]
    assert any('first_leader_id = NULL' in s for s in sql_calls)
    assert any('second_leader_id = NULL' in s for s in sql_calls)
    assert any('DELETE FROM public.researcher' in s for s in sql_calls)
    session.commit.assert_called_once()


def test_remove_researchers_aborts_on_empty_admin_list(tmp_path):
    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.content = _parquet_bytes([])
    mock_resp.raise_for_status = MagicMock()

    session = MagicMock()
    with patch('httpx.Client.get', return_value=mock_resp):
        with pytest.raises(RuntimeError, match='está vazia'):
            run_remove_researchers(session, settings=settings)

    session.execute.assert_not_called()
    session.commit.assert_not_called()


def test_remove_researchers_targeted_lattes_ids():
    session = MagicMock()
    select_res = MagicMock()
    select_res.fetchall.return_value = [('uuid-1',)]
    session.execute.side_effect = [
        select_res,
        MagicMock(),
        MagicMock(),
        MagicMock(),
    ]

    stats = run_remove_researchers(
        session,
        lattes_ids=['9999888877776666'],
    )

    assert stats.found == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    session.commit.assert_called_once()


def test_remove_researchers_none_found():
    session = MagicMock()
    select_res = MagicMock()
    select_res.fetchall.return_value = []
    session.execute.return_value = select_res

    stats = run_remove_researchers(
        session,
        researcher_ids=['uuid-nonexistent'],
    )

    assert stats.found == 0
    assert stats.succeeded == 0
    assert stats.failed == 0
    session.commit.assert_not_called()
