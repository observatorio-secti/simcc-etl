from unittest.mock import MagicMock, patch

import pytest

from simcc_etl import pipeline
from simcc_etl.common import Stats


def _make_mock_engine_and_session():
    engine = MagicMock()
    lock_conn = MagicMock()
    engine.connect.return_value = lock_conn
    session = MagicMock()
    return engine, lock_conn, session


def test_export_oda_research_groups_is_non_blocking():
    assert 'export_oda_research_groups' in pipeline.NON_BLOCKING_ROUTINES


def test_run_pipeline_continues_when_non_blocking_routine_fails(monkeypatch):
    engine, lock_conn, session = _make_mock_engine_and_session()
    executed: list[str] = []

    def failing_oda(sess, lattes_ids=None):
        executed.append('export_oda_research_groups')
        raise RuntimeError('ODA API fora do ar')

    def succeeding_next(sess, lattes_ids=None):
        executed.append('after_oda')
        return Stats(found=5, succeeded=4, failed=1)

    monkeypatch.setitem(
        pipeline.ROUTINES, 'export_oda_research_groups', failing_oda
    )
    monkeypatch.setitem(pipeline.ROUTINES, 'after_oda', succeeding_next)

    with (
        patch.object(pipeline, 'get_write_engine', return_value=engine),
        patch.object(pipeline, 'create_etl_tables'),
        patch.object(pipeline, 'try_acquire_lock', return_value=True),
        patch.object(pipeline, 'release_lock', return_value=True) as rel_mock,
        patch.object(pipeline, 'get_write_session', return_value=session),
        patch.object(pipeline, 'logger') as logger_mock,
    ):
        etl_run = pipeline.run_pipeline(
            routines=['export_oda_research_groups', 'after_oda'],
            triggered_by='test',
        )

    assert executed == ['export_oda_research_groups', 'after_oda']
    assert etl_run.status == 'SUCCEEDED'
    assert etl_run.error_message is None
    assert etl_run.current_routine is None

    oda_step = etl_run.steps['export_oda_research_groups']
    assert oda_step['status'] == 'FAILED'
    assert oda_step['error'] == 'ODA API fora do ar'
    assert oda_step['non_blocking'] is True

    next_step = etl_run.steps['after_oda']
    assert next_step['status'] == 'SUCCEEDED'
    assert next_step['found'] == 5
    assert next_step['succeeded'] == 4
    assert next_step['failed'] == 1

    assert session.rollback.called
    assert rel_mock.called
    assert lock_conn.close.called
    assert session.close.called
    assert not logger_mock.error.called
    assert any(
        'Falha na rotina' in call.args[0]
        for call in logger_mock.warning.call_args_list
    )


def test_run_pipeline_stops_when_blocking_routine_fails(monkeypatch):
    engine, lock_conn, session = _make_mock_engine_and_session()
    executed: list[str] = []

    def failing_blocking(sess, lattes_ids=None):
        executed.append('blocking_step')
        raise RuntimeError('erro crítico')

    def should_not_run(sess, lattes_ids=None):
        executed.append('after_blocking')
        return Stats(found=1, succeeded=1, failed=0)

    monkeypatch.setitem(pipeline.ROUTINES, 'blocking_step', failing_blocking)
    monkeypatch.setitem(pipeline.ROUTINES, 'after_blocking', should_not_run)

    with (
        patch.object(pipeline, 'get_write_engine', return_value=engine),
        patch.object(pipeline, 'create_etl_tables'),
        patch.object(pipeline, 'try_acquire_lock', return_value=True),
        patch.object(pipeline, 'release_lock', return_value=True) as rel_mock,
        patch.object(pipeline, 'get_write_session', return_value=session),
        pytest.raises(RuntimeError, match='erro crítico'),
    ):
        pipeline.run_pipeline(
            routines=['blocking_step', 'after_blocking'],
            triggered_by='test',
        )

    assert executed == ['blocking_step']
    assert session.rollback.called
    assert rel_mock.called
    assert lock_conn.close.called
    assert session.close.called

    added_run = session.add.call_args.args[0]
    assert added_run.status == 'FAILED'
    assert added_run.error_message == 'erro crítico'
    assert added_run.steps['blocking_step']['status'] == 'FAILED'
    assert 'non_blocking' not in added_run.steps['blocking_step']
