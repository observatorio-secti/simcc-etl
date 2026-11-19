from unittest.mock import MagicMock

import pytest

from simcc_etl.core.settings import Settings
from simcc_etl.routines import enrich_researcher_institution as eri


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        ('30', 30.0),
        ('31,6', 31.6),
        ('20 h sem', 20.0),
        ('40 h sem', 40.0),
        ('DE', 40.0),
        ('Dedc exclus', 40.0),
        ('Dedicação Exclusiva', 40.0),
        ('0', 0.0),
        ('168', 168.0),
        ('-5', None),
        ('169', None),
        ('', None),
        (None, None),
        ('sem carga', None),
    ],
)
def test_parse_workload(raw, expected):
    assert eri.parse_workload(raw) == expected


def test_reference_files_resolve_real_ba_ceps_and_territories():
    lookup = eri.CepLookup()
    territories = eri.load_identity_territories()

    assert lookup.resolve('41.150-000') == 'Salvador'
    assert lookup.resolve('47810-047') == 'Barreiras'
    assert lookup.resolve('44300-000') == 'Cachoeira'

    assert territories['SALVADOR'] == 'METROPOLITANA DE SALVADOR'
    assert territories['BARREIRAS'] == 'BACIA DO RIO GRANDE'
    assert territories['SANTOAMARO'] == 'RECÔNCAVO'


def test_missing_reference_files_raise_file_not_found(tmp_path):
    session = MagicMock()
    with pytest.raises(FileNotFoundError):
        eri.run(
            session,
            cep_ranges_path=tmp_path / 'missing_cep.csv',
        )
    with pytest.raises(FileNotFoundError):
        eri.run(
            session,
            territories_path=tmp_path / 'missing_terr.csv',
        )


def _mock_session_with_db_maps(address_rows=None):
    session = MagicMock()
    inst_result = MagicMock()
    inst_result.mappings.return_value.all.return_value = [
        {'id': 'inst-ebmsp', 'acronym': 'EBMSP', 'name': 'Escola Bahiana'},
        {'id': 'inst-ufob', 'acronym': 'UFOB', 'name': 'Universidade Federal do Oeste da Bahia'},
        {'id': 'inst-ufrb', 'acronym': 'UFRB', 'name': 'Universidade Federal do Recôncavo da Bahia'},
        {'id': 'inst-uesc', 'acronym': 'UESC', 'name': 'Universidade Estadual de Santa Cruz'},
    ]
    city_result = MagicMock()
    city_result.mappings.return_value.all.return_value = [
        {'id': 'city-ssa', 'name': 'Salvador'},
        {'id': 'city-bar', 'name': 'Barreiras'},
        {'id': 'city-santo-amaro', 'name': 'Santo Amaro'},
        {'id': 'city-ilheus', 'name': 'Ilhéus'},
    ]
    addr_result = MagicMock()
    addr_result.mappings.return_value.all.return_value = address_rows or []
    write_result = MagicMock()
    session.execute.side_effect = [
        inst_result,
        city_result,
        write_result,
        addr_result,
        write_result,
    ]
    return session


def test_run_enriches_all_three_csv_formats_and_extensible_fourth(tmp_path):
    researchers_dir = tmp_path / 'seed' / 'researchers'
    researchers_dir.mkdir(parents=True)

    (researchers_dir / 'ebmsp.csv').write_text(
        'NOME_DOCENTE;CH semanal;CEP;lattes_id\n'
        'DOCENTE 1;31,6;41.150-000;1681116663002230\n',
        encoding='utf-8',
    )
    (researchers_dir / 'ufob.csv').write_text(
        'NOME SERVIDOR;ESCOLARIDADE;JORNADA TRABALHO;NOME UORG;CEP;lattes_id\n'
        'DOCENTE 2;DOUTORADO;Dedc exclus;CCBS;47810-047;6142410058045866\n',
        encoding='utf-8',
    )
    (researchers_dir / 'ufrb.csv').write_text(
        'siape,name,department,work_regime,city,lattes_id,zip_code\n'
        '123,DOCENTE 3,CAHL,DE,SANTO AMARO DA PURIFICAÇÃO,'
        '8343393957854863,44200-000\n',
        encoding='utf-8',
    )
    (researchers_dir / 'uesc.csv').write_text(
        'id_lattes;regime;municipio;cep\n'
        '9999888877776666;20h;Ilhéus;45662-900\n',
        encoding='utf-8',
    )

    session = _mock_session_with_db_maps()
    settings = Settings(STORAGE_DIR=tmp_path)

    stats = eri.run(session, settings=settings)

    assert stats.found == 4
    assert stats.succeeded == 4
    assert stats.failed == 0
    session.commit.assert_called_once()

    # 3a chamada a execute() é o UPDATE em lote
    _, update_params = session.execute.call_args_list[2].args
    by_lattes = {row['lattes_id']: row for row in update_params}

    assert by_lattes['1681116663002230'] == {
        'lattes_id': '1681116663002230',
        'institution_id': 'inst-ebmsp',
        'identity_territory': 'METROPOLITANA DE SALVADOR',
        'workload': 31.6,
        'city_id': 'city-ssa',
    }
    assert by_lattes['6142410058045866'] == {
        'lattes_id': '6142410058045866',
        'institution_id': 'inst-ufob',
        'identity_territory': 'BACIA DO RIO GRANDE',
        'workload': 40.0,
        'city_id': 'city-bar',
    }
    assert by_lattes['8343393957854863'] == {
        'lattes_id': '8343393957854863',
        'institution_id': 'inst-ufrb',
        'identity_territory': 'RECÔNCAVO',
        'workload': 40.0,
        'city_id': 'city-santo-amaro',
    }
    assert by_lattes['9999888877776666'] == {
        'lattes_id': '9999888877776666',
        'institution_id': 'inst-uesc',
        'identity_territory': 'LITORAL SUL',
        'workload': 20.0,
        'city_id': 'city-ilheus',
    }


def test_register_institution_parser_allows_custom_override(tmp_path):
    researchers_dir = tmp_path / 'seed' / 'researchers'
    researchers_dir.mkdir(parents=True)
    (researchers_dir / 'uesc.csv').write_text(
        'lattes_id,workload,city\n1111222233334444,20,Ilhéus\n',
        encoding='utf-8',
    )

    def _custom_uesc(row):
        row['workload'] = '40'
        return row

    eri.register_institution_parser('UESC', _custom_uesc)
    try:
        session = _mock_session_with_db_maps()
        stats = eri.run(session, settings=Settings(STORAGE_DIR=tmp_path))
        assert stats.succeeded == 1
        _, update_params = session.execute.call_args_list[2].args
        assert update_params[0]['workload'] == 40.0
    finally:
        eri.INSTITUTION_PARSERS.pop('UESC', None)


def test_run_enriches_missing_researchers_from_address_cep(tmp_path):
    address_rows = [
        # Resolve instituição por nome normalizado e CEP de Salvador
        {
            'researcher_id': 'res-1',
            'lattes_id': '1111111111111111',
            'cep': '41.150-000',
            'address_institution': 'Universidade Estadual de Santa Cruz',
            'researcher_institution_id': None,
        },
        # Resolve instituição por sigla e CEP de Barreiras
        {
            'researcher_id': 'res-2',
            'lattes_id': '2222222222222222',
            'cep': '47810-047',
            'address_institution': 'ufob',
            'researcher_institution_id': None,
        },
        # Fallback para researcher.institution_id quando address_institution não casa
        {
            'researcher_id': 'res-3',
            'lattes_id': '3333333333333333',
            'cep': '44200-000',
            'address_institution': 'Instituição Desconhecida',
            'researcher_institution_id': 'inst-ufrb',
        },
        # Ignorado: sem institution_id resolvido
        {
            'researcher_id': 'res-4',
            'lattes_id': '4444444444444444',
            'cep': '41.150-000',
            'address_institution': 'Sem Cadastro',
            'researcher_institution_id': None,
        },
        # Ignorado: CEP inválido / não resolve localização
        {
            'researcher_id': 'res-5',
            'lattes_id': '5555555555555555',
            'cep': '00000-000',
            'address_institution': 'UESC',
            'researcher_institution_id': None,
        },
    ]

    session = MagicMock()
    inst_result = MagicMock()
    inst_result.mappings.return_value.all.return_value = [
        {'id': 'inst-ufob', 'acronym': 'UFOB', 'name': 'Universidade Federal do Oeste da Bahia'},
        {'id': 'inst-ufrb', 'acronym': 'UFRB', 'name': 'Universidade Federal do Recôncavo da Bahia'},
        {'id': 'inst-uesc', 'acronym': 'UESC', 'name': 'Universidade Estadual de Santa Cruz'},
    ]
    city_result = MagicMock()
    city_result.mappings.return_value.all.return_value = [
        {'id': 'city-ssa', 'name': 'Salvador'},
        {'id': 'city-bar', 'name': 'Barreiras'},
        {'id': 'city-santo-amaro', 'name': 'Santo Amaro'},
    ]
    addr_result = MagicMock()
    addr_result.mappings.return_value.all.return_value = address_rows
    insert_result = MagicMock()
    session.execute.side_effect = [
        inst_result,
        city_result,
        addr_result,
        insert_result,
    ]

    # Mesmo sem diretório seed/researchers, o fallback via researcher_address roda
    stats = eri.run(session, settings=Settings(STORAGE_DIR=tmp_path))

    assert stats.found == 5
    assert stats.succeeded == 3
    assert stats.failed == 2
    session.commit.assert_called_once()

    insert_sql, insert_params = session.execute.call_args_list[3].args
    assert 'INSERT INTO public.researcher_institution' in str(insert_sql)
    by_res = {row['researcher_id']: row for row in insert_params}

    assert by_res['res-1'] == {
        'researcher_id': 'res-1',
        'institution_id': 'inst-uesc',
        'identity_territory': 'METROPOLITANA DE SALVADOR',
        'city_id': 'city-ssa',
    }
    assert by_res['res-2'] == {
        'researcher_id': 'res-2',
        'institution_id': 'inst-ufob',
        'identity_territory': 'BACIA DO RIO GRANDE',
        'city_id': 'city-bar',
    }
    assert by_res['res-3'] == {
        'researcher_id': 'res-3',
        'institution_id': 'inst-ufrb',
        'identity_territory': 'RECÔNCAVO',
        'city_id': 'city-santo-amaro',
    }
