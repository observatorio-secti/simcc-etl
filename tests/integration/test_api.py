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
    assert isinstance(response.json(), list)


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
