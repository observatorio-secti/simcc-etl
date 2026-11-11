from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

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
