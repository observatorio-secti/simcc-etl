from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

from simcc_etl.routines.export_oda_research_groups import (
    _build_record,
    _save_groups_to_database,
    run,
    should_run_sync,
)


def test_should_run_sync_no_previous_run():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.scalar.return_value = None
    session.execute.return_value = mock_res

    assert should_run_sync(session, interval_days=7) is True


def test_should_run_sync_recent_run():
    session = MagicMock()
    mock_res = MagicMock()
    # Executou há 2 dias
    mock_res.scalar.return_value = datetime.now(timezone.utc) - timedelta(days=2)
    session.execute.return_value = mock_res

    assert should_run_sync(session, interval_days=7) is False


def test_should_run_sync_old_run():
    session = MagicMock()
    mock_res = MagicMock()
    # Executou há 8 dias
    mock_res.scalar.return_value = datetime.now(timezone.utc) - timedelta(days=8)
    session.execute.return_value = mock_res

    assert should_run_sync(session, interval_days=7) is True


def test_build_record():
    raw_group = {
        'id': 'oda-123',
        'dgpId': 'dgp-456',
        'nome': 'Grupo de Inteligência Artificial',
        'areaPredominante': 'Ciência da Computação',
        'anoFormacao': 2020,
        'situacao': 'CERTIFICADO',
        'instituicoes': [{'sigla': 'UESC', 'nome': 'Universidade Estadual de Santa Cruz', 'tipoRelacao': 'SEDE'}],
    }
    leaders = [{'name': 'Líder 1', 'lattes_id': '1111222233334444'}]

    record = _build_record(raw_group, leaders)
    assert record['oda_id'] == 'oda-123'
    assert record['dgp_id'] == 'dgp-456'
    assert record['name'] == 'Grupo de Inteligência Artificial'
    assert record['institution_acronym'] == 'UESC'
    assert record['leaders'] == leaders


def test_save_groups_to_database():
    session = MagicMock()
    # Mock do _get_researcher_lattes_map
    res_mock = MagicMock()
    res_mock.mappings.return_value.all.return_value = [
        {'id': 'uuid-res-1', 'lattes_id': '1111222233334444'}
    ]
    # Mock do upsert_group_sql returning id
    upsert_mock = MagicMock()
    upsert_mock.mappings.return_value.first.return_value = {'id': 'uuid-group-1'}

    session.execute.side_effect = [res_mock, upsert_mock, MagicMock()]

    records = [
        {
            'oda_id': 'oda-1',
            'dgp_id': 'dgp-1',
            'name': 'Grupo Teste',
            'area': 'Computação',
            'year': 2021,
            'institution_acronym': 'UESC',
            'institution_name': 'Universidade Estadual',
            'leaders': [{'name': 'Líder 1', 'lattes_id': '1111222233334444'}],
        }
    ]

    succeeded, failed = _save_groups_to_database(session, records)
    assert succeeded == 1
    assert failed == 0
    assert session.commit.called


def test_run_skips_when_recent():
    session = MagicMock()
    with patch(
        'simcc_etl.routines.export_oda_research_groups.should_run_sync',
        return_value=False,
    ):
        stats = run(session, force=False)
        assert stats.found == 0
        assert stats.succeeded == 0
        assert stats.failed == 0


def test_should_run_sync_zero_interval_returns_true():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.scalar.return_value = datetime.now(timezone.utc)
    session.execute.return_value = mock_res

    assert should_run_sync(session, interval_days=0) is True


def test_should_run_sync_sql_avoids_jsonb_question_operator():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.scalar.return_value = None
    session.execute.return_value = mock_res

    should_run_sync(session, interval_days=7)

    stmt = session.execute.call_args.args[0]
    sql_text = str(stmt)
    assert '?' not in sql_text
    assert "steps->'export_oda_research_groups'->>'status'" in sql_text


def test_run_raises_on_oda_http_502():
    session = MagicMock()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(502, request=request)
    )
    orig_client = httpx.AsyncClient

    def _mock_client(**kwargs):
        kwargs['transport'] = transport
        return orig_client(**kwargs)

    with (
        patch(
            'simcc_etl.routines.export_oda_research_groups.httpx.AsyncClient',
            side_effect=_mock_client,
        ),
        pytest.raises(httpx.HTTPStatusError),
    ):
        run(session, force=True)


def test_run_raises_on_oda_connection_error():
    session = MagicMock()

    def _raise_connect_error(request):
        raise httpx.ConnectError('connection refused', request=request)

    transport = httpx.MockTransport(_raise_connect_error)
    orig_client = httpx.AsyncClient

    def _mock_client(**kwargs):
        kwargs['transport'] = transport
        return orig_client(**kwargs)

    with (
        patch(
            'simcc_etl.routines.export_oda_research_groups.httpx.AsyncClient',
            side_effect=_mock_client,
        ),
        pytest.raises(httpx.ConnectError),
    ):
        run(session, force=True)
