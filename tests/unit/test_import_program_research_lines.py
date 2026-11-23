from pathlib import Path
from unittest.mock import MagicMock, patch

from simcc_etl.common import Stats
from simcc_etl.routines import import_program_research_lines


def _write_csv(tmp_path: Path, content: str) -> Path:
    csv_path = tmp_path / 'program_research_lines.csv'
    csv_path.write_text(content, encoding='utf-8')
    return csv_path


def test_import_program_research_lines_missing_csv(tmp_path: Path):
    session = MagicMock()
    missing_path = tmp_path / 'does_not_exist.csv'

    with patch.object(
        import_program_research_lines, 'SEED_CSV_PATH', missing_path
    ):
        stats = import_program_research_lines.run(session)

    assert stats == Stats(found=0, succeeded=0, failed=0)
    session.execute.assert_not_called()


def test_import_program_research_lines_formats_before_upsert(
    tmp_path: Path,
):
    csv_path = _write_csv(
        tmp_path,
        'Código do Programa,Programa,Nome,Área de Concentração,'
        'Data de Início,Data de Fim\n'
        '41006011002P0,ADMINISTRAÇÃO,GESTÃO DA INOVAÇÃO,'
        'Estratégia e Competitividade,01/01/2017,-\n',
    )
    session = MagicMock()
    prog_res = MagicMock()
    prog_res.mappings.return_value.all.return_value = [
        {'graduate_program_id': 'gp-uuid-1', 'code': '41006011002P0'}
    ]
    upsert_res = MagicMock()
    session.execute.side_effect = [prog_res, upsert_res]

    with patch.object(
        import_program_research_lines, 'SEED_CSV_PATH', csv_path
    ):
        stats = import_program_research_lines.run(session)

    assert stats == Stats(found=1, succeeded=1, failed=0)
    assert session.execute.call_count == 2
    sql_text = str(session.execute.call_args_list[1].args[0])
    assert 'ON CONFLICT (graduate_program_id, name) DO UPDATE' in sql_text
    assert 'UPDATE research_lines_programs SET' not in sql_text

    records = session.execute.call_args_list[1].args[1]
    assert len(records) == 1
    assert records[0]['name'] == 'Gestão Da Inovação'
    assert records[0]['area'] == 'ESTRATÉGIA E COMPETITIVIDADE'
    assert records[0]['start_year'] == 2017
    assert records[0]['end_year'] is None


def test_import_program_research_lines_deduplicates_and_skips_invalid(
    tmp_path: Path,
):
    csv_path = _write_csv(
        tmp_path,
        'Código do Programa,Programa,Nome,Área de Concentração,'
        'Data de Início,Data de Fim\n'
        '41006011002P0,ADM,LINHA A,AREA 1,01/01/2017,-\n'
        '41006011002P0,ADM,linha a,AREA 1,01/01/2018,31/12/2020\n'
        '41006011002P0,ADM,,AREA 1,01/01/2017,-\n'
        '41006011002P0,ADM,LINHA B,,01/01/2017,-\n'
        '99999999999P0,OUTRO,LINHA C,AREA 2,01/01/2017,-\n',
    )
    session = MagicMock()
    prog_res = MagicMock()
    prog_res.mappings.return_value.all.return_value = [
        {'graduate_program_id': 'gp-uuid-1', 'code': '41006011002P0'}
    ]
    upsert_res = MagicMock()
    session.execute.side_effect = [prog_res, upsert_res]

    with patch.object(
        import_program_research_lines, 'SEED_CSV_PATH', csv_path
    ):
        stats = import_program_research_lines.run(session)

    assert stats.found == 5
    assert stats.succeeded == 1
    assert stats.failed == 3
    records = session.execute.call_args_list[1].args[1]
    assert len(records) == 1
    assert records[0]['name'] == 'Linha A'
    assert records[0]['start_year'] == 2018
    assert records[0]['end_year'] == 2020
