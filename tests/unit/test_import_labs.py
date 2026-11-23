from unittest.mock import MagicMock, patch

import httpx
import pytest

from simcc_etl import pipeline
from simcc_etl.common import Stats
from simcc_etl.routines import import_labs


def test_import_labs_registered_in_daily_pipeline():
    assert 'import_labs' in pipeline.ROUTINES
    assert 'import_labs' in pipeline.DEFAULT_ROUTINES
    assert 'import_labs' in pipeline.NON_BLOCKING_ROUTINES


def test_clean_str():
    assert (
        import_labs._clean_str('  Linha 1\r\nLinha  2\t ')
        == 'Linha 1 Linha 2'
    )
    assert import_labs._clean_str('   \n\r  ') is None
    assert import_labs._clean_str(None) is None


def test_prepare_lab_params_valid_and_normalizes():
    researcher_map = {'0000123456789012': 'res-uuid-1'}
    institution_map = {'UESC': 'inst-uuid-1'}
    raw = {
        'HashedID': 'hash-123',
        'tipo': 'Pesquisa',
        'local': 'Pavilhão\nPedro Calmon',
        'nome': 'Lab de Dados',
        'descricao': 'Descrição\r\ndo lab',
        'site': 'https://lab.uesc.br',
        'atividades': 'Pesquisa\naplicada',
        'areas': 'Computação',
        'campus': 'Soane Nazaré',
        'instituicao': 'uesc',
        'id_lattes': '123456789012',
        'responsavel': 'Prof. Fulano',
    }

    params = import_labs._prepare_lab_params(
        raw, researcher_map, institution_map
    )
    assert params == {
        'hashed_id': 'hash-123',
        'type': 'Pesquisa',
        'location': 'Pavilhão Pedro Calmon',
        'name': 'Lab de Dados',
        'description': 'Descrição do lab',
        'website': 'https://lab.uesc.br',
        'activities': 'Pesquisa aplicada',
        'areas': 'Computação',
        'campus': 'Soane Nazaré',
        'institution_id': 'inst-uuid-1',
        'researcher_id': 'res-uuid-1',
        'responsible': 'Prof. Fulano',
    }


def test_prepare_lab_params_allows_missing_or_unknown_lattes():
    researcher_map = {'0000123456789012': 'res-uuid-1'}
    institution_map = {'UESC': 'inst-uuid-1'}

    params_no_lattes = import_labs._prepare_lab_params(
        {
            'HashedID': 'h1',
            'id_lattes': '',
            'instituicao': 'UESC',
            'nome': 'Lab Sem Lattes',
            'tipo': 'Pesquisa',
        },
        researcher_map,
        institution_map,
    )
    assert params_no_lattes is not None
    assert params_no_lattes['institution_id'] == 'inst-uuid-1'
    assert params_no_lattes['researcher_id'] is None

    params_unknown_lattes = import_labs._prepare_lab_params(
        {
            'HashedID': 'h2',
            'id_lattes': '999',
            'instituicao': 'UESC',
            'nome': 'Lab Lattes Desconhecido',
            'tipo': 'Pesquisa',
        },
        researcher_map,
        institution_map,
    )
    assert params_unknown_lattes is not None
    assert params_unknown_lattes['institution_id'] == 'inst-uuid-1'
    assert params_unknown_lattes['researcher_id'] is None


def test_prepare_lab_params_skips_missing_required_or_institution():
    researcher_map = {'0000123456789012': 'res-uuid-1'}
    institution_map = {'UESC': 'inst-uuid-1'}

    assert (
        import_labs._prepare_lab_params(
            {'id_lattes': '123456789012', 'instituicao': 'UESC'},
            researcher_map,
            institution_map,
        )
        is None
    )
    assert (
        import_labs._prepare_lab_params(
            {
                'HashedID': 'h1',
                'id_lattes': '123456789012',
                'instituicao': 'OUTRA',
                'nome': 'Lab',
                'tipo': 'Pesquisa',
            },
            researcher_map,
            institution_map,
        )
        is None
    )


def test_save_labs_to_database_inserts_and_updates():
    session = MagicMock()

    res_mock = MagicMock()
    res_mock.mappings.return_value.all.return_value = [
        {'id': 'res-1', 'lattes_id': '123456789012'},
    ]
    inst_mock = MagicMock()
    inst_mock.mappings.return_value.all.return_value = [
        {'id': 'inst-1', 'acronym': 'UESC'},
    ]
    existing_mock = MagicMock()
    existing_mock.mappings.return_value.all.return_value = [
        {'id': 'lab-existing-1', 'hashed_id': 'hash-existing'},
    ]

    insert_res = MagicMock()
    insert_res.scalar.return_value = 'lab-new-1'
    update_res = MagicMock()

    session.execute.side_effect = [
        res_mock,
        inst_mock,
        existing_mock,
        update_res,
        insert_res,
    ]

    labs = [
        {
            'HashedID': 'hash-existing',
            'tipo': 'Pesquisa',
            'nome': 'Lab Existente',
            'instituicao': 'UESC',
            'id_lattes': '123456789012',
        },
        {
            'HashedID': 'hash-new',
            'tipo': 'Ensino',
            'nome': 'Lab Novo Sem Lattes',
            'instituicao': 'UESC',
            'id_lattes': '',
        },
        {
            'HashedID': 'hash-invalid',
            'tipo': 'Ensino',
            'nome': 'Lab Sem Instituição',
            'instituicao': 'DESCONHECIDA',
            'id_lattes': '0000000000000000',
        },
    ]

    succeeded, failed = import_labs._save_labs_to_database(session, labs)

    assert succeeded == 2
    assert failed == 1
    assert session.commit.called
    assert 'UPDATE public.labs' in str(
        session.execute.call_args_list[3].args[0]
    )
    assert 'INSERT INTO public.labs' in str(
        session.execute.call_args_list[4].args[0]
    )


def test_run_fetches_and_filters_by_lattes_ids():
    api_url = 'https://vip.uesc.br/list_labs_json'
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {'HashedID': 'h1', 'id_lattes': '1111222233334444'},
        {'HashedID': 'h2', 'id_lattes': '5555666677778888'},
    ]
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_resp

    session = MagicMock()
    with (
        patch('simcc_etl.routines.import_labs.Settings') as mock_settings,
        patch(
            'simcc_etl.routines.import_labs.httpx.Client',
            return_value=mock_client,
        ),
        patch(
            'simcc_etl.routines.import_labs._save_labs_to_database',
            return_value=(1, 0),
        ) as save_mock,
    ):
        mock_settings.return_value.LABS_API_URL = api_url
        stats = import_labs.run(session, lattes_ids=['1111222233334444'])

    assert stats == Stats(found=1, succeeded=1, failed=0)
    saved_labs = save_mock.call_args.args[1]
    assert len(saved_labs) == 1
    assert saved_labs[0]['HashedID'] == 'h1'


def test_run_raises_on_http_error():
    api_url = 'https://vip.uesc.br/list_labs_json'
    req = httpx.Request('GET', api_url)
    resp = httpx.Response(503, request=req)
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        'Service Unavailable', request=req, response=resp
    )
    mock_client = MagicMock()
    mock_client.__enter__.return_value.get.return_value = mock_resp

    session = MagicMock()
    with (
        patch('simcc_etl.routines.import_labs.Settings') as mock_settings,
        patch(
            'simcc_etl.routines.import_labs.httpx.Client',
            return_value=mock_client,
        ),
    ):
        mock_settings.return_value.LABS_API_URL = api_url
        with pytest.raises(httpx.HTTPStatusError):
            import_labs.run(session)
