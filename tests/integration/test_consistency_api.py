from datetime import datetime, timedelta, timezone

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from simcc_etl.app import app
from simcc_etl.core.database import ConsistencyReport, create_etl_tables
from simcc_etl.core.settings import Settings
from simcc_etl.routers import get_db

API_KEY = Settings().ETL_API_KEY
HEADERS = {'X-ETL-API-Key': API_KEY}


@pytest.fixture
def client():
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool,
        future=True,
    )
    create_etl_tables(engine)
    sm = sessionmaker(engine, expire_on_commit=False)

    def _override_get_db():
        session = sm()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as test_client:
        yield test_client, sm
    app.dependency_overrides.clear()


def test_consistency_requires_api_key(client):
    test_client, _ = client
    assert (
        test_client.get('/consistency').status_code
        == status.HTTP_401_UNAUTHORIZED
    )
    assert (
        test_client.get('/consistencia/export').status_code
        == status.HTTP_401_UNAUTHORIZED
    )


def test_manual_create_edit_and_summary(client):
    test_client, _ = client

    create_resp = test_client.post(
        '/consistency',
        headers=HEADERS,
        json={
            'type': 'capes_scraping',
            'status': 'COMPLETED',
            'coverage': 'FULL',
            'reference_at': '2026-11-10T12:00:00Z',
            'before': {'files': 0},
            'after': {'files': 3},
            'result': {'note': 'Raspagem Sucupira 2026'},
            'recorded_by': 'admin@simcc',
        },
    )
    assert create_resp.status_code == status.HTTP_201_CREATED
    created = create_resp.json()
    assert created['type'] == 'capes_scraping'
    assert created['origin'] == 'manual'
    assert created['result']['_audit']['created_by'] == 'admin@simcc'

    patch_resp = test_client.patch(
        f'/consistency/{created["id"]}',
        headers=HEADERS,
        json={
            'reference_at': '2026-11-11T09:30:00Z',
            'result': {'note': 'Data corrigida após conferência'},
            'edited_by': 'gestor@simcc',
            'edit_note': 'Ajuste de horário da raspagem',
        },
    )
    assert patch_resp.status_code == status.HTTP_200_OK
    updated = patch_resp.json()
    assert updated['reference_at'].startswith('2026-11-11T09:30:00')
    assert updated['result']['note'] == 'Data corrigida após conferência'
    assert len(updated['result']['_audit']['edits']) == 1
    assert updated['result']['_audit']['edits'][0]['edited_by'] == (
        'gestor@simcc'
    )

    summary_resp = test_client.get(
        '/consistency', headers=HEADERS, params={'type': 'capes_scraping'}
    )
    assert summary_resp.status_code == status.HTTP_200_OK
    summary = summary_resp.json()
    assert len(summary) == 1
    assert summary[0]['health'] == 'OK'
    assert summary[0]['last_complete_success']['id'] == created['id']


def test_history_and_portuguese_export_contract(client):
    test_client, sm = client
    now = datetime.now(timezone.utc)

    with sm() as session:
        session.add_all([
            ConsistencyReport(
                id='oa-ok',
                type='openalex',
                origin='cli',
                status='COMPLETED',
                coverage='FULL',
                started_at=now - timedelta(days=1),
                ended_at=now - timedelta(days=1),
                before={'unseen_pending_articles': 10},
                after={'unseen_pending_articles': 0},
                result={'delta_unseen_pending_articles': -10},
            ),
            ConsistencyReport(
                id='oa-fail',
                type='openalex',
                origin='api',
                status='FAILED',
                coverage='NA',
                started_at=now - timedelta(hours=1),
                ended_at=now - timedelta(hours=1),
                before={'unseen_pending_articles': 4},
                after=None,
                result={'routine': 'enrich_from_openalex'},
                error='HTTP 429',
            ),
            ConsistencyReport(
                id='qualis-ok',
                type='qualis',
                origin='manual',
                status='COMPLETED',
                coverage='FULL',
                started_at=now - timedelta(hours=2),
                ended_at=now - timedelta(hours=2),
                before={},
                after={'periodical_magazines': 500},
                result={'fonte': 'qualis-2021-2024.xlsx'},
            ),
        ])
        session.commit()

    # Summary keeps both failure and last complete success visible
    summary_resp = test_client.get(
        '/consistency', headers=HEADERS, params={'type': 'openalex'}
    )
    assert summary_resp.status_code == status.HTTP_200_OK
    oa_summary = summary_resp.json()[0]
    assert oa_summary['health'] == 'CRITICAL'
    assert oa_summary['last_attempt']['id'] == 'oa-fail'
    assert oa_summary['last_complete_success']['id'] == 'oa-ok'

    # History endpoint
    hist_resp = test_client.get(
        '/consistency/types/openalex/history', headers=HEADERS
    )
    assert hist_resp.status_code == status.HTTP_200_OK
    assert [r['id'] for r in hist_resp.json()] == ['oa-fail', 'oa-ok']

    # Portuguese export endpoint
    export_resp = test_client.get(
        '/consistencia/export',
        headers=HEADERS,
        params={'apenas_ultimos': True},
    )
    assert export_resp.status_code == status.HTTP_200_OK
    exported = export_resp.json()
    expected_keys = {
        'id',
        'pipeline_log_id',
        'escopo',
        'estado',
        'iniciado_em',
        'finalizado_em',
        'antes',
        'depois',
        'resultado',
        'erro',
    }
    assert {item['id'] for item in exported} == {
        'oa-fail',
        'oa-ok',
        'qualis-ok',
    }
    for item in exported:
        assert set(item.keys()) == expected_keys
        assert item['escopo'] == 'simcc'

    by_id = {item['id']: item for item in exported}
    assert by_id['oa-fail']['estado'] == 'FALHA'
    assert by_id['oa-fail']['erro'] == 'HTTP 429'
    assert by_id['oa-ok']['estado'] == 'CONCLUIDO'
    assert by_id['oa-ok']['resultado']['tipo'] == 'openalex'
