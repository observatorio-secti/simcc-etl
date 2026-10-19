from simcc_etl.routines.import_capes_programs import (
    process_program_row,
)


def test_process_program_row_ignored_situacao():
    row = {'situacao': 'DESATIVADO'}
    data, err = process_program_row(row, {})
    assert data is None
    assert 'Ignorado' in str(err)


def test_process_program_row_institution_not_found():
    row = {
        'situacao': 'EM FUNCIONAMENTO',
        'ies_nome': 'Universidade Desconhecida',
    }
    data, err = process_program_row(row, {})
    assert data is None
    assert 'Instituição não encontrada' in str(err)


def test_process_program_row_valid():
    inst_map = {'universidade do estado da bahia': 'uneb-uuid'}
    row = {
        'situacao': 'EM FUNCIONAMENTO',
        'ies_nome': 'Universidade do Estado da Bahia',
        'ies_municipio': 'Salvador - BA',
        'codigo': '31001017001P0',
        'nome': 'DIFUSÃO DO CONHECIMENTO',
        'cursos': (
            "[{'situacao': 'em funcionamento', 'nota': '5', 'nivel': 'D'}]"
        ),
    }
    data, err = process_program_row(row, inst_map)
    assert err is None
    assert data is not None
    assert data['code'] == '31001017001P0'
    assert data['institution_id'] == 'uneb-uuid'
    assert data['city'] == 'Salvador'
    assert data['state'] == 'BA'
