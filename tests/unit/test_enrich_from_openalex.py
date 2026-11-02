import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from simcc_etl.routines.enrich_from_openalex import (
    _build_abstract,  # noqa: PLC2701
    _get_or_fetch_payload,  # noqa: PLC2701
    _safe_get,  # noqa: PLC2701
    run,
)


@pytest.fixture
def sample_article_payload():
    return {
        'abstract_inverted_index': {'Machine': [0], 'learning': [1]},
        'authorships': [{'author': {'display_name': 'Alan Turing'}}],
        'primary_location': {
            'source': {'display_name': 'Nature AI', 'issn': ['1234-5678']},
            'landing_page_url': 'https://example.com/nature',
            'pdf_url': 'https://example.com/nature.pdf',
        },
        'cited_by_count': 42,
        'language': 'en',
    }


@pytest.fixture
def sample_researcher_payload():
    return {
        'id': 'https://openalex.org/A123456789',
        'summary_stats': {'h_index': 15, 'i10_index': 20},
        'works_count': 85,
        'cited_by_count': 1200,
        'ids': {'scopus': '57200000000'},
    }


def test_build_abstract_helper():
    assert _build_abstract(None) is None
    assert _build_abstract({}) is None
    inv = {'hello': [0], 'world': [1]}
    assert _build_abstract(inv) == 'hello world'


def test_safe_get_helper():
    data = {'a': {'b': {'c': 10}}}
    assert _safe_get(data, 'a', 'b', 'c') == 10
    assert _safe_get(data, 'a', 'x') is None
    assert _safe_get(None, 'a') is None


def test_get_or_fetch_payload_uses_disk_cache(
    tmp_path, sample_article_payload
):
    cache_file = tmp_path / 'cached.json'
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(sample_article_payload, f)

    mock_client = MagicMock()
    payload = _get_or_fetch_payload(
        mock_client, 'https://api.test', cache_file
    )

    assert payload == sample_article_payload
    mock_client.get.assert_not_called()


def test_get_or_fetch_payload_fetches_when_no_cache(
    tmp_path, sample_article_payload
):
    cache_file = tmp_path / 'new.json'
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_article_payload
    mock_client.get.return_value = mock_resp

    payload = _get_or_fetch_payload(
        mock_client, 'https://api.test', cache_file
    )

    assert payload == sample_article_payload
    mock_client.get.assert_called_once_with('https://api.test')
    assert cache_file.is_file()
    with open(cache_file, 'r', encoding='utf-8') as f:
        assert json.load(f) == sample_article_payload


def test_get_or_fetch_payload_corrupted_cache_falls_back(
    tmp_path, sample_article_payload
):
    cache_file = tmp_path / 'corrupt.json'
    cache_file.write_text('{bad json')

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_article_payload
    mock_client.get.return_value = mock_resp

    payload = _get_or_fetch_payload(
        mock_client, 'https://api.test', cache_file
    )

    assert payload == sample_article_payload
    mock_client.get.assert_called_once_with('https://api.test')


def test_enrich_from_openalex_batch_commit(tmp_path, sample_article_payload):
    """Testa se o commit é disparado a cada lote e ao final (residual)."""
    session = MagicMock()

    # 5 artigos pendentes
    art_rows = [{'id': f'art_{i}', 'doi': f'10.1000/{i}'} for i in range(5)]

    def mock_execute(sql, *args, **kwargs):
        sql_str = str(sql)
        res = MagicMock()
        if 'FROM bibliographic_production' in sql_str:
            res.mappings.return_value.all.return_value = art_rows
        elif 'FROM researcher' in sql_str:
            res.mappings.return_value.all.return_value = []
        return res

    session.execute.side_effect = mock_execute

    with patch('httpx.Client.get') as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = sample_article_payload
        mock_get.return_value = mock_resp

        # batch_size=2: espera commit após 2, após 4 e residual (total 3)
        stats = run(session, batch_size=2, storage_dir=tmp_path)

        assert stats.found == 5
        assert stats.succeeded == 5
        assert stats.failed == 0
        assert session.commit.call_count == 3


def test_enrich_from_openalex_exact_multiple_batch_commit(
    tmp_path, sample_article_payload
):
    """Testa lote exato de 50 registros sem commit residual redundante."""
    session = MagicMock()

    art_rows = [{'id': f'art_{i}', 'doi': f'10.1000/{i}'} for i in range(50)]

    def mock_execute(sql, *args, **kwargs):
        sql_str = str(sql)
        res = MagicMock()
        if 'FROM bibliographic_production' in sql_str:
            res.mappings.return_value.all.return_value = art_rows
        elif 'FROM researcher' in sql_str:
            res.mappings.return_value.all.return_value = []
        return res

    session.execute.side_effect = mock_execute

    with patch('httpx.Client.get') as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = sample_article_payload
        mock_get.return_value = mock_resp

        stats = run(session, batch_size=50, storage_dir=tmp_path)

        assert stats.found == 50
        assert stats.succeeded == 50
        assert stats.failed == 0
        # Exatamente 50 itens com batch_size=50 -> 1 commit
        assert session.commit.call_count == 1


def test_enrich_from_openalex_researchers_cache_and_db_rollback(
    tmp_path, sample_researcher_payload
):
    """Testa tratamento de erro no DB com rollback e isolamento."""
    session = MagicMock()

    res_rows = [
        {'id': 'res_ok', 'orcid': '0000-0001-2345-6789'},
        {'id': 'res_fail', 'orcid': '0000-0002-3456-7890'},
        {'id': 'res_ok2', 'orcid': '0000-0003-4567-8901'},
    ]

    # Pre-cria cache para o primeiro pesquisador
    res_dir = tmp_path / 'researcher'
    res_dir.mkdir(parents=True, exist_ok=True)
    with open(res_dir / 'res_ok.json', 'w', encoding='utf-8') as f:
        json.dump(sample_researcher_payload, f)

    def mock_execute(sql, params=None, *args, **kwargs):
        sql_str = str(sql)
        res = MagicMock()
        if 'FROM bibliographic_production' in sql_str:
            res.mappings.return_value.all.return_value = []
        elif 'FROM researcher' in sql_str:
            res.mappings.return_value.all.return_value = res_rows
        elif 'INSERT INTO openalex_researcher' in sql_str:
            if params and params.get('researcher_id') == 'res_fail':
                raise OperationalError('Simulated DB deadlock', {}, None)
        return res

    session.execute.side_effect = mock_execute

    with patch('httpx.Client.get') as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = sample_researcher_payload
        mock_get.return_value = mock_resp

        stats = run(session, batch_size=50, storage_dir=tmp_path)

        assert stats.found == 3
        assert stats.succeeded == 2
        assert stats.failed == 1
        # Verifica rollback chamado na falha
        assert session.rollback.call_count == 1
        # Verifica commit dos sucessos residuais
        assert session.commit.call_count >= 1
        # O pesquisador que estava em cache não deve ter chamado a API
        # Apenas res_fail e res_ok2 chamam a API
        assert mock_get.call_count == 2
