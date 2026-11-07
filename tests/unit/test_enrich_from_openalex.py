import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from simcc_etl.routines.enrich_from_openalex import (
    _build_abstract,  # noqa: PLC2701
    _clean_orcid,  # noqa: PLC2701
    _get_or_fetch_payload,  # noqa: PLC2701
    _partition_unseen_and_retry,  # noqa: PLC2701
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


def test_clean_orcid_helper():
    assert _clean_orcid(None) is None
    assert _clean_orcid('') is None
    assert _clean_orcid('   ') is None
    assert _clean_orcid('0000-0001-2345-6789') == '0000-0001-2345-6789'
    orcid_url = 'https://orcid.org/0000-0001-2345-6789'
    assert _clean_orcid(orcid_url) == '0000-0001-2345-6789'
    http_orcid_url = 'http://orcid.org/0000-0001-2345-6789'
    assert _clean_orcid(http_orcid_url) == '0000-0001-2345-6789'
    raw_prefix = 'orcid.org/0000-0001-2345-6789'
    assert _clean_orcid(raw_prefix) == '0000-0001-2345-6789'


def test_enrich_from_openalex_researchers_cache_and_db_rollback(
    tmp_path, sample_researcher_payload
):
    """Testa tratamento de erro no DB com savepoint e isolamento."""
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
        # Verifica isolamento de savepoint para cada inserção
        assert session.begin_nested.call_count == 3
        # Verifica commit dos sucessos residuais
        assert session.commit.call_count >= 1
        # O pesquisador que estava em cache não deve ter chamado a API
        # Apenas res_fail e res_ok2 chamam a API
        assert mock_get.call_count == 2


def test_api_fetch_failures_do_not_reset_batch_counter(
    tmp_path, sample_article_payload
):
    """Testa se falhas de API não resetam o contador de commit de lote."""
    session = MagicMock()

    # 5 artigos: art_0 ok, art_1 fail, art_2 ok, art_3 fail, art_4 ok
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

    def mock_client_get(url):
        mock_resp = MagicMock()
        if '10.1000/1' in url or '10.1000/3' in url:
            mock_resp.status_code = 404
            mock_resp.json.side_effect = ValueError('Not Found')
        else:
            mock_resp.status_code = 200
            mock_resp.json.return_value = sample_article_payload
        return mock_resp

    with patch('httpx.Client.get', side_effect=mock_client_get):
        # batch_size=2: mesmo com falhas, comita ao atingir 2 sucessos
        # e mais 1 commit residual para o 3º sucesso (total 2 commits)
        stats = run(session, batch_size=2, storage_dir=tmp_path)

        assert stats.found == 5
        assert stats.succeeded == 3
        assert stats.failed == 2
        # Commit de lote ao atingir 2 sucessos + 1 residual no final
        assert session.commit.call_count == 2


def test_partition_unseen_and_retry_orders_404_last(tmp_path):
    """Testa particionamento jogando registros com 404 para o fim."""
    not_found_dir = tmp_path / 'not_found'
    not_found_dir.mkdir(parents=True)
    # Marca art_1 e art_3 como 404
    (not_found_dir / 'art_1.json').write_text('{"status_code": 404}')
    (not_found_dir / 'art_3.json').write_text('{"status_code": 404}')

    rows = [{'id': f'art_{i}'} for i in range(5)]
    ordered = _partition_unseen_and_retry(rows, not_found_dir)

    ordered_ids = [r['id'] for r in ordered]
    # art_0, art_2, art_4 (inéditos) primeiro; art_1, art_3 no final
    assert ordered_ids == ['art_0', 'art_2', 'art_4', 'art_1', 'art_3']


def test_get_or_fetch_payload_records_and_cleans_404(
    tmp_path, sample_article_payload
):
    """Testa escrita de 404 no not_found e limpeza posterior ao obter 200."""
    cache_file = tmp_path / 'cache' / 'art_1.json'
    not_found_file = tmp_path / 'not_found' / 'art_1.json'
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    not_found_file.parent.mkdir(parents=True, exist_ok=True)

    mock_client = MagicMock()
    mock_resp_404 = MagicMock()
    mock_resp_404.status_code = 404
    mock_client.get.return_value = mock_resp_404

    # 1. Requisição 404
    payload = _get_or_fetch_payload(
        mock_client, 'https://api.test/1', cache_file, not_found_file
    )
    assert payload is None
    assert not cache_file.is_file()
    assert not_found_file.is_file()
    data = json.loads(not_found_file.read_text())
    assert data['status_code'] == 404
    assert 'attempted_at' in data

    # 2. Requisição 200 posterior (recuperação)
    mock_resp_200 = MagicMock()
    mock_resp_200.status_code = 200
    mock_resp_200.json.return_value = sample_article_payload
    mock_client.get.return_value = mock_resp_200

    payload = _get_or_fetch_payload(
        mock_client, 'https://api.test/1', cache_file, not_found_file
    )
    assert payload == sample_article_payload
    assert cache_file.is_file()
    # not_found_file deve ter sido removido
    assert not not_found_file.is_file()


def test_run_prioritizes_unseen_before_retry_404(
    tmp_path, sample_article_payload
):
    """Testa ponta a ponta que inéditos rodam antes dos que já deram 404."""
    session = MagicMock()

    # Pre-cria not_found para art_0
    not_found_dir = tmp_path / 'not_found' / 'article'
    not_found_dir.mkdir(parents=True, exist_ok=True)
    (not_found_dir / 'art_0.json').write_text('{"status_code": 404}')

    # art_0 (já 404), art_1 (inédito), art_2 (inédito)
    art_rows = [
        {'id': 'art_0', 'doi': '10.1000/0'},
        {'id': 'art_1', 'doi': '10.1000/1'},
        {'id': 'art_2', 'doi': '10.1000/2'},
    ]

    def mock_execute(sql, *args, **kwargs):
        sql_str = str(sql)
        res = MagicMock()
        if 'FROM bibliographic_production' in sql_str:
            res.mappings.return_value.all.return_value = art_rows
        elif 'FROM researcher' in sql_str:
            res.mappings.return_value.all.return_value = []
        return res

    session.execute.side_effect = mock_execute

    called_urls = []

    def mock_client_get(url):
        called_urls.append(url)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = sample_article_payload
        return mock_resp

    with patch('httpx.Client.get', side_effect=mock_client_get):
        stats = run(session, storage_dir=tmp_path)

        assert stats.found == 3
        assert stats.succeeded == 3
        # Ordem de execução deve ser art_1, art_2 (inéditos) e art_0 por último
        assert len(called_urls) == 3
        assert '10.1000/1' in called_urls[0]
        assert '10.1000/2' in called_urls[1]
        assert '10.1000/0' in called_urls[2]
