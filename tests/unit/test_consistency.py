from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from simcc_etl.common import Stats
from simcc_etl.core.consistency import (
    COVERAGE_FULL,
    COVERAGE_PARTIAL,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PARTIAL,
    build_type_summaries,
    compute_deltas,
    consistency_types_for_routine,
    resolve_completion,
)
from simcc_etl.core.database import (
    Base,
    ConsistencyReport,
    EtlRun,
    create_etl_tables,
)
from simcc_etl.pipeline import ROUTINES, run_pipeline


def _make_memory_engine():
    engine = create_engine('sqlite:///:memory:', future=True)
    create_etl_tables(engine)
    return engine


def test_consistency_types_for_routine_granularity():
    assert consistency_types_for_routine('calculate_production_index') == (
        'indprod_researchers',
        'indprod_graduate_programs',
        'indprod_research_groups',
    )
    assert consistency_types_for_routine(
        'calculate_graduate_program_indprod'
    ) == ('indprod_graduate_programs',)
    assert consistency_types_for_routine('enrich_from_openalex') == (
        'openalex',
    )


def test_compute_deltas():
    before = {'enriched': 10, 'pending': 5, 'flag': True}
    after = {'enriched': 14, 'pending': 1, 'flag': False}
    assert compute_deltas(before, after) == {
        'delta_enriched': 4,
        'delta_pending': -4,
    }


def test_resolve_completion_openalex_rules():
    # Filtered run is always PARTIAL.
    assert resolve_completion(
        'openalex',
        Stats(found=2, succeeded=2, failed=0),
        lattes_ids=['123'],
        after_snapshot={
            'unseen_pending_articles': 0,
            'unseen_pending_researchers': 0,
        },
    ) == (STATUS_PARTIAL, COVERAGE_PARTIAL)

    # Full sweep with no remaining unseen items is COMPLETED/FULL even when
    # OpenAlex returned 404 (counted as stats.failed and cached in not_found).
    assert resolve_completion(
        'openalex',
        Stats(found=10, succeeded=8, failed=2),
        after_snapshot={
            'unseen_pending_articles': 0,
            'unseen_pending_researchers': 0,
        },
    ) == (STATUS_COMPLETED, COVERAGE_FULL)

    # Full sweep with remaining unseen items stays PARTIAL.
    assert resolve_completion(
        'openalex',
        Stats(found=10, succeeded=5, failed=5),
        after_snapshot={
            'unseen_pending_articles': 3,
            'unseen_pending_researchers': 0,
        },
    ) == (STATUS_PARTIAL, COVERAGE_PARTIAL)


def test_build_type_summaries_does_not_mask_recent_failure():
    engine = _make_memory_engine()
    sm = sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)

    with sm() as session:
        session.add_all([
            ConsistencyReport(
                id='old-ok',
                type='openalex',
                origin='cli',
                status=STATUS_COMPLETED,
                coverage=COVERAGE_FULL,
                started_at=now - timedelta(days=2),
                ended_at=now - timedelta(days=2),
                before={},
            ),
            ConsistencyReport(
                id='recent-fail-1',
                type='openalex',
                origin='cli',
                status=STATUS_FAILED,
                coverage='NA',
                started_at=now - timedelta(hours=3),
                ended_at=now - timedelta(hours=3),
                before={},
                error='rate limit',
            ),
            ConsistencyReport(
                id='recent-fail-2',
                type='openalex',
                origin='api',
                status=STATUS_FAILED,
                coverage='NA',
                started_at=now - timedelta(hours=1),
                ended_at=now - timedelta(hours=1),
                before={},
                error='timeout',
            ),
        ])
        session.commit()

        summaries = build_type_summaries(session, filter_type='openalex')

    assert len(summaries) == 1
    item = summaries[0]
    assert item['health'] == 'CRITICAL'
    assert item['consecutive_failures'] == 2
    assert item['attempts_last_7d'] == 3
    assert item['last_attempt'].id == 'recent-fail-2'
    assert item['last_complete_success'].id == 'old-ok'


def test_run_pipeline_records_granular_and_partial_reports(monkeypatch):
    engine = _make_memory_engine()
    sm = sessionmaker(engine, expire_on_commit=False)

    def ok_routine(session, lattes_ids=None):
        return Stats(found=5, succeeded=5, failed=0)

    def failing_routine(session, lattes_ids=None):
        raise RuntimeError('boom')

    monkeypatch.setitem(ROUTINES, 'calculate_production_index', ok_routine)
    monkeypatch.setitem(ROUTINES, 'sanitize_lattes_data', failing_routine)

    with (
        patch('simcc_etl.pipeline.get_write_engine', return_value=engine),
        patch(
            'simcc_etl.pipeline.get_write_session',
            side_effect=lambda eng=None: sm(),
        ),
        patch('simcc_etl.pipeline.try_acquire_lock', return_value=True),
        patch('simcc_etl.pipeline.release_lock', return_value=True),
    ):
        etl_run = run_pipeline(
            routines=['calculate_production_index'],
            triggered_by='cli',
        )
        run_pipeline(
            routines=['calculate_production_index'],
            lattes_ids=['0001'],
            triggered_by='api',
        )
        with pytest.raises(RuntimeError, match='boom'):
            run_pipeline(
                routines=['sanitize_lattes_data'],
                triggered_by='cli',
            )

    with sm() as session:
        reports = session.scalars(
            select(ConsistencyReport).order_by(ConsistencyReport.started_at)
        ).all()

    assert len(reports) == 7
    full_run_reports = [r for r in reports if r.etl_run_id == etl_run.id]
    assert {r.type for r in full_run_reports} == {
        'indprod_researchers',
        'indprod_graduate_programs',
        'indprod_research_groups',
    }
    assert all(r.status == STATUS_COMPLETED for r in full_run_reports)
    assert all(r.coverage == COVERAGE_FULL for r in full_run_reports)

    partial_reports = [r for r in reports if r.origin == 'api']
    assert len(partial_reports) == 3
    assert all(r.status == STATUS_PARTIAL for r in partial_reports)
    assert all(r.coverage == COVERAGE_PARTIAL for r in partial_reports)

    failed_reports = [r for r in reports if r.type == 'sanitize_lattes']
    assert len(failed_reports) == 1
    assert failed_reports[0].status == STATUS_FAILED
    assert failed_reports[0].error == 'boom'

    # Keep Base imported and verified cleanly
    assert EtlRun.__tablename__ in Base.metadata.tables
