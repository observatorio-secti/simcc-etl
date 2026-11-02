from uuid import uuid4

from fastapi import status
from fastapi.testclient import TestClient

from simcc_etl.app import app

client = TestClient(app)


def test_root_redirect():
    response = client.get('/', follow_redirects=False)
    assert response.status_code in {
        status.HTTP_307_TEMPORARY_REDIRECT,
        status.HTTP_302_FOUND,
    }
    assert response.headers['location'] == '/dashboard'


def test_dashboard_html_view():
    response = client.get('/dashboard')
    assert response.status_code == status.HTTP_200_OK
    assert 'text/html' in response.headers['content-type']
    assert 'SimCC ETL Monitor' in response.text


def test_dashboard_api_runs():
    response = client.get('/dashboard/api/runs')
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)


def test_dashboard_api_run_not_found():
    random_id = str(uuid4())
    response = client.get(f'/dashboard/api/runs/{random_id}')
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_dashboard_api_run_logs_empty():
    random_id = str(uuid4())
    response = client.get(f'/dashboard/api/runs/{random_id}/logs')
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data['lines'] == []
    assert data['next_offset'] == 0


def test_dashboard_api_daily_list():
    response = client.get('/dashboard/api/daily')
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)


def test_dashboard_api_daily_logs_not_found():
    response = client.get('/dashboard/api/daily/1999-01-01/logs')
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data['lines'] == []
    assert data['next_offset'] == 0
