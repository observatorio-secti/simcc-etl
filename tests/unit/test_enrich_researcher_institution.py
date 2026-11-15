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


def _mock_session_with_db_maps():
    session = MagicMock()
    inst_result = MagicMock()
    inst_result.mappings.return_value.all.return_value = [
        {'id': 'inst-ebmsp', 'acronym': 'EBMSP'},
        {'id': 'inst-ufob', 'acronym': 'UFOB'},
        {'id': 'inst-ufrb', 'acronym': 'UFRB'},
        {'id': 'inst-uesc', 'acronym': 'UESC'},
    ]
    city_result = MagicMock()
    city_result.mappings.return_value.all.return_value = [
        {'id': 'city-ssa', 'name': 'Salvador'},
        {'id': 'city-bar', 'name': 'Barreiras'},
        {'id': 'city-santo-amaro', 'name': 'Santo Amaro'},
        {'id': 'city-ilheus', 'name': 'Ilhéus'},
    ]
    update_result = MagicMock()
    session.execute.side_effect = [inst_result, city_result, update_result]
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
