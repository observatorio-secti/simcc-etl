from uuid import uuid4

from fastapi import status
from fastapi.testclient import TestClient

from simcc_etl.app import app
from simcc_etl.core.settings import Settings

client = TestClient(app)
API_KEY = Settings().ETL_API_KEY


def test_health_check():
    response = client.get('/health')
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {'status': 'ok'}


def test_list_routines_unauthorized():
    response = client.get('/routines')
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_list_routines_authorized():
    response = client.get('/routines', headers={'X-ETL-API-Key': API_KEY})
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    ids = {item['id'] for item in data}
    assert 'indprod' not in ids
    assert 'refresh_search_views' not in ids
    assert 'publish_to_read_db' in ids
    first = data[0]
    assert {'id', 'title', 'description', 'category'} <= set(first.keys())
    assert first['category'] in {
        'collection',
        'processing',
        'consolidation',
        'indicators',
        'integration',
    }


def test_openapi_declares_routine_category_enum():
    schema = app.openapi()
    routine_schema = schema['components']['schemas']['RoutineInfo']
    assert routine_schema['properties']['category']['$ref'].endswith(
        '/RoutineCategory'
    )
    enum_schema = schema['components']['schemas']['RoutineCategory']
    assert enum_schema['enum'] == [
        'collection',
        'processing',
        'consolidation',
        'indicators',
        'integration',
    ]


def test_get_run_logs_empty():
    dummy_id = uuid4()
    response = client.get(
        f'/runs/{dummy_id}/logs',
        headers={'X-ETL-API-Key': API_KEY},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data['lines'] == []
    assert data['next_offset'] == 0
    assert data['total'] == 0


def test_get_run_logs_paginated_with_total(tmp_path, monkeypatch):
    run_id = uuid4()
    runs_dir = tmp_path / 'runs'
    runs_dir.mkdir(parents=True)
    log_file = runs_dir / f'{run_id}.jsonl'
    log_file.write_text(
        '{"event":"step 1","level":"info"}\n'
        '{"event":"step 2","level":"info"}\n'
        '{"event":"step 3","level":"info"}\n',
        encoding='utf-8',
    )
    monkeypatch.setenv('LOG_DIR', str(tmp_path))

    response = client.get(
        f'/runs/{run_id}/logs?offset=1&limit=1',
        headers={'X-ETL-API-Key': API_KEY},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data['lines'] == [{'event': 'step 2', 'level': 'info'}]
    assert data['next_offset'] == 2
    assert data['total'] == 3
