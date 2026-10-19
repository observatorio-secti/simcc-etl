import io
import zipfile

from simcc_etl.routines.soap_lattes import process_lattes_content


def test_process_lattes_content_valid_xml(tmp_path):
    curr_dir = tmp_path / 'current'
    run_dir = tmp_path / 'run'
    curr_dir.mkdir()
    run_dir.mkdir()

    xml_content = (
        b'<?xml version="1.0" encoding="ISO-8859-1"?>'
        b'<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708">'
        b'<DADOS-GERAIS NOME-COMPLETO="Hugo Saba"/>'
        b'</CURRICULO-VITAE>'
    )

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as z:
        z.writestr('1966167015825708.xml', xml_content)

    success = process_lattes_content(
        '1966167015825708',
        zip_buffer.getvalue(),
        curr_dir,
        run_dir,
    )

    assert success is True
    assert (curr_dir / '1966167015825708.xml').exists()
    assert (run_dir / '1966167015825708.xml').exists()


def test_process_lattes_content_invalid_zip(tmp_path):
    curr_dir = tmp_path / 'current'
    run_dir = tmp_path / 'run'
    curr_dir.mkdir()
    run_dir.mkdir()

    success = process_lattes_content(
        '1966167015825708',
        b'not a zip content',
        curr_dir,
        run_dir,
    )
    assert success is False
