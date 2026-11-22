from unittest.mock import MagicMock, patch

import pytest

from simcc_etl.routines.publish_to_read_db import (
    EXCLUDED_TABLES,
    PublishResult,
    _record_mv_refresh_metadata,
    bump_redis_cache,
    compare_schemas,
    mview_creation_order,
    refresh_materialized_views,
    retarget_fk_def,
    retarget_index_def,
    run,
)

STAGING = 'simcc_publish_staging'


def test_retarget_index_def_moves_table_reference_only():
    indexdef = (
        'CREATE INDEX idx_title ON public.article USING gin '
        '(public.f_unaccent(title) public.gin_trgm_ops)'
    )
    assert retarget_index_def(indexdef, STAGING) == (
        'CREATE INDEX idx_title ON "simcc_publish_staging".article USING gin '
        '(public.f_unaccent(title) public.gin_trgm_ops)'
    )


def test_retarget_index_def_unique_index():
    indexdef = 'CREATE UNIQUE INDEX uq_code ON public.graduate_program (code)'
    assert retarget_index_def(indexdef, STAGING).startswith(
        'CREATE UNIQUE INDEX uq_code ON "simcc_publish_staging".'
    )


def test_retarget_index_def_rejects_unknown_format():
    with pytest.raises(RuntimeError):
        retarget_index_def('CREATE INDEX x ON other.t (a)', STAGING)


def test_retarget_fk_def_only_for_published_tables():
    fkdef = (
        'FOREIGN KEY (researcher_id) REFERENCES public.researcher(id) '
        'ON DELETE CASCADE'
    )
    assert retarget_fk_def(fkdef, STAGING, {'researcher'}) == (
        'FOREIGN KEY (researcher_id) REFERENCES '
        '"simcc_publish_staging".researcher(id) ON DELETE CASCADE'
    )
    assert retarget_fk_def(fkdef, STAGING, {'other'}) == fkdef


def test_mview_creation_order_respects_dependencies():
    mviews = {'mv_a': '...', 'mv_b': '...', 'mv_c': '...'}
    deps = {'mv_a': {'mv_b', 'some_table'}, 'mv_b': {'mv_c'}}
    order = mview_creation_order(mviews, deps)
    assert order.index('mv_c') < order.index('mv_b') < order.index('mv_a')


def test_compare_schemas_identical():
    tables = {'researcher': [('id', 'uuid'), ('name', 'text')]}
    assert compare_schemas(tables, tables) == []


def test_compare_schemas_reports_drift():
    write = {
        'researcher': [('id', 'uuid'), ('name', 'text')],
        'only_write': [('id', 'uuid')],
        'etl_runs': [('id', 'uuid')],
        'consistency_reports': [('id', 'text')],
    }
    read = {
        'researcher': [('id', 'uuid'), ('name', 'character varying')],
        'only_read': [('id', 'uuid')],
    }
    problems = compare_schemas(write, read)
    assert 'tabela only_write existe só no write_db' in problems
    assert 'tabela only_read existe só no read_db' in problems
    assert any('researcher.name' in p for p in problems)
    # Tabelas excluídas da publicação não contam como divergência.
    assert not any('etl_runs' in p for p in problems)
    assert not any('consistency_reports' in p for p in problems)


def test_bump_redis_cache_success():
    with patch('redis.Redis.from_url') as mock_redis_from_url:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.incr.return_value = 42
        mock_redis_from_url.return_value = mock_client

        res = bump_redis_cache('redis://localhost:6379/0')
        assert res is True
        mock_client.incr.assert_called_once_with('simcc:v2:search:generation')


def test_bump_redis_cache_failure():
    with patch('redis.Redis.from_url') as mock_redis_from_url:
        mock_redis_from_url.side_effect = Exception('Redis connection error')
        res = bump_redis_cache('redis://localhost:6379/0')
        assert res is False


def test_run_publishes_via_staging(monkeypatch):
    monkeypatch.setenv('WRITE_DATABASE_URL', 'postgresql://u@w:5432/write')
    monkeypatch.setenv('READ_DATABASE_URL', 'postgresql://u@r:5432/read')
    monkeypatch.setenv('REDIS_URL', 'redis://localhost:6379/0')
    with (
        patch(
            'simcc_etl.routines.publish_to_read_db.publish_via_staging',
            return_value=PublishResult(tables=81, mviews=15),
        ) as mock_publish,
        patch(
            'simcc_etl.routines.publish_to_read_db.bump_redis_cache'
        ) as mock_bump,
    ):
        stats = run(MagicMock())

    mock_publish.assert_called_once()
    mock_bump.assert_called_once()
    assert (stats.found, stats.succeeded, stats.failed) == (96, 96, 0)


def test_run_same_database_only_refreshes(monkeypatch):
    dsn = 'postgresql://u@db:5432/simcc'
    monkeypatch.setenv('WRITE_DATABASE_URL', dsn)
    monkeypatch.setenv('READ_DATABASE_URL', dsn)
    monkeypatch.setenv('REDIS_URL', '')
    with (
        patch(
            'simcc_etl.routines.publish_to_read_db.publish_via_staging'
        ) as mock_publish,
        patch(
            'simcc_etl.routines.publish_to_read_db.refresh_materialized_views',
            return_value=15,
        ),
    ):
        stats = run(MagicMock())

    mock_publish.assert_not_called()
    assert stats.succeeded == 15


def test_mv_refresh_metadata_is_excluded_from_schema_comparison():
    assert 'mv_refresh_metadata' in EXCLUDED_TABLES
    write = {'researcher': [('id', 'uuid')]}
    read = {
        'researcher': [('id', 'uuid')],
        'mv_refresh_metadata': [('view_name', 'text')],
    }
    assert compare_schemas(write, read) == []


def test_record_mv_refresh_metadata_skips_when_table_missing():
    cur = MagicMock()
    cur.fetchone.return_value = (False,)

    _record_mv_refresh_metadata(cur, {'mv_a': 120})

    cur.execute.assert_called_once()


def test_record_mv_refresh_metadata_upserts_when_table_exists():
    cur = MagicMock()
    cur.fetchone.return_value = (True,)

    _record_mv_refresh_metadata(cur, {'mv_a': 120, 'mv_b': 45})

    assert cur.execute.call_count == 3
    params = [c.args[1] for c in cur.execute.call_args_list[1:]]
    assert params == [('mv_a', 120), ('mv_b', 45)]


def test_refresh_materialized_views_records_metadata():
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [('mv_a', 'SELECT 1'), ('mv_b', 'SELECT 2')],
        [],
    ]
    cur.fetchone.return_value = (True,)
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur

    with patch(
        'simcc_etl.routines.publish_to_read_db._connect',
        return_value=conn,
    ):
        count = refresh_materialized_views('postgresql://u@r:5432/read')

    assert count == 2
    insert_calls = [
        c
        for c in cur.execute.call_args_list
        if isinstance(c.args[0], str)
        and 'INSERT INTO public.mv_refresh_metadata' in c.args[0]
    ]
    assert len(insert_calls) == 2
    assert {c.args[1][0] for c in insert_calls} == {'mv_a', 'mv_b'}
