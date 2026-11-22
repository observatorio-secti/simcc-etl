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

    packaged = {p.name for p in eri.DEFAULT_RESEARCHERS_DIR.glob('*.csv')}
    assert {'ebmsp.csv', 'ufob.csv', 'ufrb.csv'}.issubset(packaged)


def test_missing_reference_files_raise_file_not_found(tmp_path, monkeypatch):
    session = MagicMock()
    with monkeypatch.context() as m:
        m.setattr(eri, 'DEFAULT_CEP_RANGES_PATH', tmp_path / 'missing_cep.csv')
        with pytest.raises(FileNotFoundError):
            eri.run(session, settings=Settings(STORAGE_DIR=tmp_path))

    with monkeypatch.context() as m:
        m.setattr(
            eri, 'DEFAULT_TERRITORIES_PATH', tmp_path / 'missing_terr.csv'
        )
        with pytest.raises(FileNotFoundError):
            eri.run(session, settings=Settings(STORAGE_DIR=tmp_path))

    with pytest.raises(FileNotFoundError):
        eri.run(
            session,
            settings=Settings(STORAGE_DIR=tmp_path),
            researchers_dir=tmp_path / 'empty_researchers',
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
    rpe_result = MagicMock()
    rpe_result.mappings.return_value.all.return_value = []
    write_result = MagicMock()
    write_result.rowcount = 0
    session.execute.side_effect = [
        inst_result,
        city_result,
        write_result,
        addr_result,
        write_result,
        write_result,
        write_result,
        rpe_result,
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

    stats = eri.run(
        session, settings=settings, researchers_dir=researchers_dir
    )

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
        stats = eri.run(
            session,
            settings=Settings(STORAGE_DIR=tmp_path),
            researchers_dir=researchers_dir,
        )
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
    rpe_result = MagicMock()
    rpe_result.mappings.return_value.all.return_value = []
    write_result = MagicMock()
    write_result.rowcount = 0
    session.execute.side_effect = [
        inst_result,
        city_result,
        addr_result,
        write_result,
        write_result,
        rpe_result,
    ]

    researchers_dir = tmp_path / 'researchers'
    researchers_dir.mkdir()
    (researchers_dir / 'uesc.csv').write_text(
        'lattes_id\n', encoding='utf-8'
    )

    # Sem pesquisadores nos CSVs e sem seed/researchers, o fallback roda
    stats = eri.run(
        session,
        settings=Settings(STORAGE_DIR=tmp_path),
        researchers_dir=researchers_dir,
    )

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


def _make_ctx():
    return eri.EnrichmentContext(
        cep_lookup=eri.CepLookup(),
        territories=eri.load_identity_territories(),
        city_ids={
            'SALVADOR': 'city-ssa',
            'FEIRADESANTANA': 'city-fds',
            'ILHEUS': 'city-ilh',
            'VALENCA': 'city-val',
        },
        institution_by_acronym={
            'EBMSP': 'inst-ebmsp',
            'UEFS': 'inst-uefs',
            'UESC': 'inst-uesc',
            'UFBA': 'inst-ufba',
        },
        institution_by_name={
            'EBMSP': 'inst-ebmsp',
            'UEFS': 'inst-uefs',
            'UESC': 'inst-uesc',
            'UFBA': 'inst-ufba',
        },
    )


def test_resolve_address_location_city_uf_fallback_and_homonym_guard():
    ctx = _make_ctx()

    # CEP genérico 40000-000 falha no CepLookup, mas cidade+UF='BA' resolve
    assert eri._resolve_address_location(
        '40000-000', 'Salvador', 'BA', ctx
    ) == ('city-ssa', 'METROPOLITANA DE SALVADOR')

    # Homônimo fora da BA (ex.: Valença/RJ) não recebe Território de Identidade da BA
    assert eri._resolve_address_location(None, 'Valença', 'RJ', ctx) == (
        'city-val',
        None,
    )


def test_stage_address_enrichment_updates_existing_nulls_and_guards_multilink():
    ctx = _make_ctx()
    address_rows = [
        # Pesquisador com 1 vínculo existente sem território -> atualiza
        {
            'researcher_id': 'res-single',
            'lattes_id': '1111111111111111',
            'cep': '40000-000',
            'city': 'Salvador',
            'uf': 'BA',
            'address_institution': 'Outra Nomeclatura',
            'researcher_institution_id': None,
            'existing_institution_id': 'inst-ufba',
            'link_count': 1,
        },
        # Multi-vínculo: endereço bate com UFBA -> atualiza UFBA
        {
            'researcher_id': 'res-multi',
            'lattes_id': '2222222222222222',
            'cep': '41.150-000',
            'city': 'Salvador',
            'uf': 'BA',
            'address_institution': 'UFBA',
            'researcher_institution_id': None,
            'existing_institution_id': 'inst-ufba',
            'link_count': 2,
        },
        # Multi-vínculo: endereço é UFBA, vínculo é UEFS -> NÃO sobrescreve UEFS
        {
            'researcher_id': 'res-multi',
            'lattes_id': '2222222222222222',
            'cep': '41.150-000',
            'city': 'Salvador',
            'uf': 'BA',
            'address_institution': 'UFBA',
            'researcher_institution_id': None,
            'existing_institution_id': 'inst-uefs',
            'link_count': 2,
        },
    ]
    session = MagicMock()
    addr_res = MagicMock()
    addr_res.mappings.return_value.all.return_value = address_rows
    update_res = MagicMock()
    session.execute.side_effect = [addr_res, update_res]

    found, succeeded, failed = eri._stage_address_enrichment(session, ctx)
    assert found == 3
    assert succeeded == 2
    assert failed == 1

    update_sql, update_params = session.execute.call_args_list[1].args
    assert 'UPDATE public.researcher_institution' in str(update_sql)
    assert len(update_params) == 2
    assert {
        (p['researcher_id'], p['institution_id']) for p in update_params
    } == {('res-single', 'inst-ufba'), ('res-multi', 'inst-ufba')}


def test_stage_institution_headquarters_updates_single_center_only():
    ctx = _make_ctx()
    session = MagicMock()
    res1 = MagicMock(rowcount=10)
    res2 = MagicMock(rowcount=20)
    res3 = MagicMock(rowcount=30)
    session.execute.side_effect = [res1, res2, res3]

    updated = eri._stage_institution_headquarters(session, ctx)
    assert updated == 60
    assert session.execute.call_count == 3
    called_insts = [
        call.args[1]['institution_id']
        for call in session.execute.call_args_list
    ]
    assert called_insts == ['inst-ebmsp', 'inst-uefs', 'inst-uesc']


def test_stage_rpe_workload_prioritizes_formal_and_filters_noise():
    ctx = _make_ctx()
    rpe_rows = [
        # Atividade Livre parcial (10h) vs vínculo Servidor_Publico (DE -> 40h)
        {
            'researcher_id': 'res-1',
            'institution_id': 'inst-ufba',
            'lattes_id': '1111111111111111',
            'enterprise': 'UFBA',
            'employment_type': 'Livre',
            'other_employment_type': 'Professor',
            'other_functional_classification': None,
            'workload_hours_weekly': 10,
            'exclusive_dedication': False,
            'start_year': 2024,
        },
        {
            'researcher_id': 'res-1',
            'institution_id': 'inst-ufba',
            'lattes_id': '1111111111111111',
            'enterprise': 'UFBA',
            'employment_type': 'Servidor_Publico',
            'other_employment_type': 'Servidor Público',
            'other_functional_classification': None,
            'workload_hours_weekly': None,
            'exclusive_dedication': True,
            'start_year': 2015,
        },
        # Livre válido (não ruído) -> 20h
        {
            'researcher_id': 'res-2',
            'institution_id': 'inst-uefs',
            'lattes_id': '2222222222222222',
            'enterprise': 'UEFS',
            'employment_type': 'Livre',
            'other_employment_type': 'Professor',
            'other_functional_classification': None,
            'workload_hours_weekly': 20,
            'exclusive_dedication': False,
            'start_year': 2020,
        },
        # Ruído: Bolsista -> descartado
        {
            'researcher_id': 'res-3',
            'institution_id': 'inst-uesc',
            'lattes_id': '3333333333333333',
            'enterprise': 'UESC',
            'employment_type': 'Livre',
            'other_employment_type': 'Bolsista',
            'other_functional_classification': None,
            'workload_hours_weekly': 40,
            'exclusive_dedication': False,
            'start_year': 2022,
        },
        # Enterprise de outra instituição -> descartado
        {
            'researcher_id': 'res-4',
            'institution_id': 'inst-uesc',
            'lattes_id': '4444444444444444',
            'enterprise': 'UFBA',
            'employment_type': 'Servidor_Publico',
            'other_employment_type': None,
            'other_functional_classification': None,
            'workload_hours_weekly': 40,
            'exclusive_dedication': False,
            'start_year': 2018,
        },
    ]

    session = MagicMock()
    rpe_res = MagicMock()
    rpe_res.mappings.return_value.all.return_value = rpe_rows
    update_res = MagicMock()
    session.execute.side_effect = [rpe_res, update_res]

    found, succeeded, failed = eri._stage_rpe_workload(session, ctx)
    assert found == 5
    assert succeeded == 2
    assert failed == 2

    update_sql, update_params = session.execute.call_args_list[1].args
    assert 'UPDATE public.researcher_institution' in str(update_sql)
    by_res = {row['researcher_id']: row for row in update_params}
    assert by_res['res-1']['workload'] == 40.0
    assert by_res['res-2']['workload'] == 20.0
