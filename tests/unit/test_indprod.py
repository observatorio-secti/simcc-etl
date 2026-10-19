from unittest.mock import MagicMock

from simcc_etl.routines.calculate_production_index import (
    BAREMA,
    fetch_patents,
    run_all,
    run_graduate_program_indprod,
    run_research_group_indprod,
    run_researcher_indprod,
)


def test_barema_weights():
    assert BAREMA['A1'] == 1.0
    assert BAREMA['B1'] == 0.5
    assert BAREMA['PATENT_GRANTED'] == 1.0
    assert BAREMA['PATENT_NOT_GRANTED'] == 0.25
    assert BAREMA['SOFTWARE'] == 0.25


def test_fetch_patents_separation():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = [
        {
            'year': 2020,
            'patent_type': 'PATENT_GRANTED',
            'researcher_id': 'res-1',
            'count_patent': 2,
        },
        {
            'year': 2021,
            'patent_type': 'PATENT_NOT_GRANTED',
            'researcher_id': 'res-1',
            'count_patent': 4,
        },
    ]
    session.execute.return_value = mock_res

    granted_df, not_granted_df = fetch_patents(session)

    assert not granted_df.is_empty()
    assert not not_granted_df.is_empty()
    assert granted_df['patent_prod_granted'][0] == 2.0  # 2 * 1.0
    assert not_granted_df['patent_prod_not_granted'][0] == 1.0  # 4 * 0.25


def test_run_researcher_indprod_empty():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = []
    session.execute.return_value = mock_res

    stats = run_researcher_indprod(session)
    assert stats.found == 0
    assert stats.succeeded == 0
    assert stats.failed == 0


def test_run_graduate_program_indprod_empty():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = []
    session.execute.return_value = mock_res

    stats = run_graduate_program_indprod(session)
    assert stats.found == 0
    assert stats.succeeded == 0
    assert stats.failed == 0


def test_run_research_group_indprod_empty():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = []
    session.execute.return_value = mock_res

    stats = run_research_group_indprod(session)
    assert stats.found == 0
    assert stats.succeeded == 0
    assert stats.failed == 0


def test_run_all_empty():
    session = MagicMock()
    mock_res = MagicMock()
    mock_res.mappings.return_value.all.return_value = []
    session.execute.return_value = mock_res

    stats = run_all(session)
    assert stats.found == 0
    assert stats.succeeded == 0
    assert stats.failed == 0
