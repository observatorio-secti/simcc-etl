from unittest.mock import MagicMock, patch

from simcc_etl.routines.publish_to_read_db import (
    SEARCH_MATERIALIZED_VIEWS,
    bump_redis_cache,
    copy_tables_to_read_db,
    refresh_materialized_views,
    run,
)


def test_copy_tables_identical_dsn():
    copied = copy_tables_to_read_db(
        'postgresql://test:test@localhost:5432/db',
        'postgresql://test:test@localhost:5432/db',
    )
    assert copied == 0


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


def test_refresh_materialized_views_success():
    with patch('psycopg.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_connect.return_value = mock_conn

        succeeded, failed = refresh_materialized_views(
            'postgresql://test:test@localhost:5432/read_db'
        )
        assert succeeded == len(SEARCH_MATERIALIZED_VIEWS)
        assert failed == 0


def test_publish_to_read_db_flow():
    session = MagicMock()
    with (
        patch(
            'simcc_etl.routines.publish_to_read_db.copy_tables_to_read_db'
        ) as mock_copy,
        patch(
            'simcc_etl.routines.publish_to_read_db.refresh_materialized_views'
        ) as mock_refresh,
        patch(
            'simcc_etl.routines.publish_to_read_db.bump_redis_cache'
        ) as mock_bump,
    ):
        mock_copy.return_value = 10
        mock_refresh.return_value = (15, 0)
        mock_bump.return_value = True

        stats = run(session)
        assert stats.found == 10 + 15
        assert stats.succeeded == 25
        assert stats.failed == 0
