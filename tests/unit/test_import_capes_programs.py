from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from simcc_etl.routines.import_capes_programs import (
    get_institutions_mapping,
    process_program_row,
    run_researchers,
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
        result['instituto federal de educacao, ciencia e tecnologia da bahia']
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
        'cursos': ("[{'situacao': 'DESATIVADO', 'nota': '5', 'nivel': 'D'}]"),
    }
    data, err = process_program_row(row, inst_map)
    assert err is None
    assert data is not None
    assert data['visible'] is False


def test_run_researchers_inserts_only_current_year(tmp_path):
    csv_path = tmp_path / 'program_researchers.csv'
    csv_path.write_text(
        'nome,id do programa,categoria\n'
        'Maria Silva,31001017001P0,COLABORADOR\n'
        'Maria Silva,31001017001P0,PERMANENTE\n'
        'Joao Souza,31001017001P0,COLABORADOR\n'
        'Desconhecido,31001017001P0,PERMANENTE\n',
        encoding='utf-8',
    )

    res_exec = MagicMock()
    res_exec.mappings.return_value.all.return_value = [
        {'id': 'res-1', 'name': 'Maria Silva'},
        {'id': 'res-2', 'name': 'Joao Souza'},
    ]
    prog_exec = MagicMock()
    prog_exec.mappings.return_value.all.return_value = [
        {'id': 'pg-1', 'code': '31001017001P0'},
    ]
    session = MagicMock()
    session.execute.side_effect = [
        res_exec,
        prog_exec,
        MagicMock(),
        MagicMock(),
    ]

    settings_mock = MagicMock()
    settings_mock.seed_dir = tmp_path

    with patch(
        'simcc_etl.routines.import_capes_programs.Settings',
        return_value=settings_mock,
    ):
        stats = run_researchers(session)

    expected_year = datetime.now(timezone.utc).year
    assert stats.found == 4
    assert stats.succeeded == 2
    assert stats.failed == 1

    insert_call = session.execute.call_args_list[-1]
    inserted_records = insert_call.args[1]
    assert inserted_records == [
        {
            'graduate_program_id': 'pg-1',
            'researcher_id': 'res-1',
            'year': expected_year,
            'type_': 'PERMANENTE',
        },
        {
            'graduate_program_id': 'pg-1',
            'researcher_id': 'res-2',
            'year': expected_year,
            'type_': 'COLABORADOR',
        },
    ]
    session.commit.assert_called_once()


def test_run_researchers_missing_csv(tmp_path):
    session = MagicMock()
    settings_mock = MagicMock()
    settings_mock.seed_dir = tmp_path

    with patch(
        'simcc_etl.routines.import_capes_programs.Settings',
        return_value=settings_mock,
    ):
        stats = run_researchers(session)

    assert stats.found == 0
    assert stats.succeeded == 0
    assert stats.failed == 0
    session.execute.assert_not_called()
