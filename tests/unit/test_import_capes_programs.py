from unittest.mock import MagicMock

from simcc_etl.routines.import_capes_programs import (
    get_institutions_mapping,
    process_program_row,
)


def test_get_institutions_mapping_requires_acronym():
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = [
        {
            'id': 'inst-1',
            'name': 'Universidade Estadual de Feira de Santana',
            'acronym': 'UEFS',
        },
        {
            'id': 'inst-2',
            'name': (
                'PONTIFÍCIA UNIVERSIDADE CATÓLICA DE MINAS GERAIS (PUC Minas)'
            ),
            'acronym': 'PUC Minas',
        },
        {
            'id': 'inst-3',
            'name': (
                'INSTITUTO FEDERAL DE EDUCAÇÃO, '
                'CIÊNCIA E TECNOLOGIA DA BAHIA (IFBA)'
            ),
            'acronym': 'IFBA',
        },
    ]

    result = get_institutions_mapping(session)

    sql_arg = str(session.execute.call_args[0][0])
    assert 'acronym IS NOT NULL' in sql_arg
    assert "TRIM(acronym) <> ''" in sql_arg
    assert result['universidade estadual de feira de santana'] == 'inst-1'
    assert (
        result['pontificia universidade catolica de minas gerais'] == 'inst-2'
    )
    assert (
        result['pontificia universidade catolica de minas gerais (puc minas)']
        == 'inst-2'
    )
    assert (
        result[
            'instituto federal de educacao, ciencia e tecnologia da bahia'
        ]
        == 'inst-3'
    )
    assert (
        result[
            'instituto federal de educacao, '
            'ciencia e tecnologia da bahia (ifba)'
        ]
        == 'inst-3'
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
    assert data['visible'] is True


def test_process_program_row_courses_uppercase_and_json():
    inst_map = {'universidade do estado da bahia': 'uneb-uuid'}
    row = {
        'situacao': 'EM FUNCIONAMENTO',
        'ies_nome': 'Universidade do Estado da Bahia',
        'ies_municipio': 'Salvador - BA',
        'codigo': '31001017001P0',
        'nome': 'DIFUSÃO DO CONHECIMENTO',
        'cursos': (
            '[{"SITUACAO": "EM FUNCIONAMENTO", "nota": "5", "nivel": "D"}]'
        ),
    }
    data, err = process_program_row(row, inst_map)
    assert err is None
    assert data is not None
    assert data['visible'] is True


def test_process_program_row_courses_inactive():
    inst_map = {'universidade do estado da bahia': 'uneb-uuid'}
    row = {
        'situacao': 'EM FUNCIONAMENTO',
        'ies_nome': 'Universidade do Estado da Bahia',
        'ies_municipio': 'Salvador - BA',
        'codigo': '31001017001P0',
        'nome': 'DIFUSÃO DO CONHECIMENTO',
        'cursos': (
            "[{'situacao': 'DESATIVADO', 'nota': '5', 'nivel': 'D'}]"
        ),
    }
    data, err = process_program_row(row, inst_map)
    assert err is None
    assert data is not None
    assert data['visible'] is False
