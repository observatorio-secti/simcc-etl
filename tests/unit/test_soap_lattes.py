import io
import os
import time
import zipfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import polars as pl
import pytest

from simcc_etl.core.settings import Settings
from simcc_etl.routines.soap_lattes import (
    _get_target_lattes_ids,  # noqa: PLC2701
    fetch_lattes_last_update,
    get_local_xml_last_update,
    process_lattes_content,
)


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


def test_get_target_lattes_ids_with_explicit_param():
    input_ids = ['1966167015825708', '1966167015825708', 'abc', '123']
    result = _get_target_lattes_ids(None, input_ids)
    assert result == {'1966167015825708': 1, '0000000000000123': 1}


def test_get_target_lattes_ids_from_admin_parquet_success():
    fake_df = pl.DataFrame({
        'lattes_id': [
            '1111222233334444',
            '5555666677778888',
            '1111222233334444',
        ]
    })
    buf = io.BytesIO()
    fake_df.write_parquet(buf)
    parquet_bytes = buf.getvalue()

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.content = parquet_bytes
    mock_resp.raise_for_status = MagicMock()

    settings = Settings(
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    with patch('httpx.Client.get', return_value=mock_resp) as mock_get:
        result = _get_target_lattes_ids(None, None, settings=settings)

        mock_get.assert_called_once_with(
            'http://test-admin:8009/academic/researchers/export/parquet',
            headers={'X-Service-Token': 'test-token'},
        )
        assert result == {'1111222233334444': 1, '5555666677778888': 1}


def test_get_target_lattes_ids_with_refresh_interval_days():
    fake_df = pl.DataFrame({
        'lattes_id': [
            '1111222233334444',
            '5555666677778888',
            '1111222233334444',
        ],
        'refresh_interval_days': [30, 7, 1],
    })
    buf = io.BytesIO()
    fake_df.write_parquet(buf)
    parquet_bytes = buf.getvalue()

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.content = parquet_bytes
    mock_resp.raise_for_status = MagicMock()

    settings = Settings(
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    with patch('httpx.Client.get', return_value=mock_resp):
        result = _get_target_lattes_ids(None, None, settings=settings)
        # 1111222233334444 appears with 30 and 1 -> min is 1
        assert result == {'1111222233334444': 1, '5555666677778888': 7}


def test_get_target_lattes_ids_empty_raises_runtime_error():
    fake_df = pl.DataFrame({'lattes_id': pl.Series([], dtype=pl.Utf8)})
    buf = io.BytesIO()
    fake_df.write_parquet(buf)
    parquet_bytes = buf.getvalue()

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.content = parquet_bytes
    mock_resp.raise_for_status = MagicMock()

    settings = Settings(
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    with patch('httpx.Client.get', return_value=mock_resp):
        with pytest.raises(RuntimeError, match='Abortando execução'):
            _get_target_lattes_ids(None, None, settings=settings)


def test_get_target_lattes_ids_http_failure_raises_runtime_error():
    settings = Settings(
        ADMIN_API_URL='http://test-admin:8009',
        ADMIN_SERVICE_TOKEN='test-token',
    )

    with patch(
        'httpx.Client.get',
        side_effect=httpx.ConnectError('Connection refused'),
    ):
        with pytest.raises(
            RuntimeError, match='Erro ao obter pesquisadores do simcc-admin'
        ):
            _get_target_lattes_ids(None, None, settings=settings)


def test_soap_lattes_ttl_cache_valid(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    run_dir = tmp_path / 'xml' / 'run_current'
    curr_dir.mkdir(parents=True)

    # Cria arquivo em cache com mtime recente
    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708"/>'
    )

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 7},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip'
        ) as mock_download,
    ):
        stats = run_soap(
            None, lattes_ids=['1966167015825708'], settings=settings
        )

        assert stats.found == 1
        assert stats.succeeded == 1
        assert stats.failed == 0
        mock_download.assert_not_called()
        assert (run_dir / '1966167015825708.xml').exists()


def test_soap_lattes_ttl_cache_expired_triggers_download(tmp_path):
    import os
    import time

    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    curr_dir.mkdir(parents=True)

    # Cria arquivo em cache com mtime antigo (10 dias atras)
    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708"/>'
    )
    old_time = time.time() - (10 * 86400)
    os.utime(xml_file, (old_time, old_time))

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=2,
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 1},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip',
            return_value=b'fake_zip',
        ) as mock_download,
        patch(
            'simcc_etl.routines.soap_lattes.process_lattes_content',
            return_value=True,
        ) as mock_process,
    ):
        stats = run_soap(
            None, lattes_ids=['1966167015825708'], settings=settings
        )

        assert stats.found == 1
        assert stats.succeeded == 1
        assert stats.failed == 0
        mock_download.assert_called_once()
        mock_process.assert_called_once()


def test_soap_lattes_parallel_download_multiple(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=4,
    )

    targets = {
        '1111111111111111': 1,
        '2222222222222222': 1,
        '3333333333333333': 1,
        '4444444444444444': 1,
    }

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value=targets,
        ),
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip',
            side_effect=lambda lid, s, client=None: b'fake_zip_'
            + lid.encode(),
        ) as mock_download,
        patch(
            'simcc_etl.routines.soap_lattes.process_lattes_content',
            return_value=True,
        ) as mock_process,
    ):
        stats = run_soap(None, settings=settings)

        assert stats.found == 4
        assert stats.succeeded == 4
        assert stats.failed == 0
        assert mock_download.call_count == 4
        assert mock_process.call_count == 4


def test_get_local_xml_last_update_valid_and_missing(tmp_path):
    xml_ok = tmp_path / 'ok.xml'
    xml_ok.write_bytes(
        b'<?xml version="1.0" encoding="ISO-8859-1"?>'
        b'<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        b'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942">'
        b'<DADOS-GERAIS NOME-COMPLETO="Jo\xe3o"/>'
        b'</CURRICULO-VITAE>'
    )
    assert get_local_xml_last_update(xml_ok) == datetime(
        2026, 11, 14, 18, 59, 42
    )

    xml_missing = tmp_path / 'missing.xml'
    xml_missing.write_text('<CURRICULO-VITAE NUMERO-IDENTIFICADOR="123"/>')
    assert get_local_xml_last_update(xml_missing) is None

    xml_bad = tmp_path / 'bad.xml'
    xml_bad.write_text('not xml')
    assert get_local_xml_last_update(xml_bad) is None


@pytest.mark.parametrize(
    ('payload', 'expected'),
    [
        ('14/11/2026 18:59:42', datetime(2026, 11, 14, 18, 59, 42)),
        ('"14/11/2026 18:59:42"', datetime(2026, 11, 14, 18, 59, 42)),
        (
            '{"data_atualizacao": "14/11/2026 18:59:42"}',
            datetime(2026, 11, 14, 18, 59, 42),
        ),
        ('data-invalida', None),
    ],
)
def test_fetch_lattes_last_update_proxy_formats(payload, expected):
    settings = Settings(
        ALTERNATIVE_CNPQ_SERVICE=True,
        CNPQ_PROXY_URL='https://proxy.example.com',
    )
    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.text = payload
    mock_resp.raise_for_status = MagicMock()
    mock_client = MagicMock(spec=httpx.Client)
    mock_client.get.return_value = mock_resp

    result = fetch_lattes_last_update(
        '1966167015825708', settings, client=mock_client
    )

    mock_client.get.assert_called_once_with(
        'https://proxy.example.com/getDataAtualizacaoCV'
        '?lattes_id=1966167015825708'
    )
    assert result == expected


def test_fetch_lattes_last_update_http_error_returns_none():
    settings = Settings(
        ALTERNATIVE_CNPQ_SERVICE=True,
        CNPQ_PROXY_URL='https://proxy.example.com',
    )
    mock_client = MagicMock(spec=httpx.Client)
    mock_client.get.side_effect = httpx.ConnectError('timeout')

    result = fetch_lattes_last_update(
        '1966167015825708', settings, client=mock_client
    )
    assert result is None


def test_fetch_lattes_last_update_soap_wsdl():
    settings = Settings(ALTERNATIVE_CNPQ_SERVICE=False)
    mock_soap_client = MagicMock()
    mock_soap_client.service.getDataAtualizacaoCV.return_value = (
        '14/11/2026 18:59:42'
    )

    with patch(
        'simcc_etl.routines.soap_lattes.Client',
        return_value=mock_soap_client,
    ):
        result = fetch_lattes_last_update('1966167015825708', settings)

    mock_soap_client.service.getDataAtualizacaoCV.assert_called_once_with(
        '1966167015825708'
    )
    assert result == datetime(2026, 11, 14, 18, 59, 42)


def test_soap_lattes_ttl_valid_skips_remote_date_check(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    curr_dir.mkdir(parents=True)
    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942"/>'
    )

    settings = Settings(STORAGE_DIR=tmp_path, ADMIN_SERVICE_TOKEN='test')

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 30},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.fetch_lattes_last_update'
        ) as mock_last_update,
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip'
        ) as mock_download,
    ):
        stats = run_soap(None, settings=settings)

    assert stats.found == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    mock_last_update.assert_not_called()
    mock_download.assert_not_called()


def test_soap_lattes_unchanged_remote_skips_zip_and_refreshes_mtime(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    run_dir = tmp_path / 'xml' / 'run_current'
    curr_dir.mkdir(parents=True)

    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942"/>'
    )
    old_time = time.time() - (31 * 86400)
    os.utime(xml_file, (old_time, old_time))

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=2,
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 30},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.fetch_lattes_last_update',
            return_value=datetime(2026, 11, 14, 18, 59, 42),
        ) as mock_last_update,
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip'
        ) as mock_download,
    ):
        stats = run_soap(None, settings=settings)

    assert stats.found == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    mock_last_update.assert_called_once()
    mock_download.assert_not_called()
    assert (run_dir / '1966167015825708.xml').exists()
    # Garante que mtime foi renovado para reiniciar o ciclo de 30 dias
    assert (time.time() - xml_file.stat().st_mtime) < 60.0


def test_soap_lattes_newer_remote_triggers_zip_download(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    curr_dir.mkdir(parents=True)

    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942"/>'
    )
    old_time = time.time() - (2 * 86400)
    os.utime(xml_file, (old_time, old_time))

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=2,
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 1},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.fetch_lattes_last_update',
            return_value=datetime(2026, 11, 15, 9, 0, 0),
        ) as mock_last_update,
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip',
            return_value=b'fake_zip',
        ) as mock_download,
        patch(
            'simcc_etl.routines.soap_lattes.process_lattes_content',
            return_value=True,
        ) as mock_process,
    ):
        stats = run_soap(None, settings=settings)

    assert stats.found == 1
    assert stats.succeeded == 1
    assert stats.failed == 0
    mock_last_update.assert_called_once()
    mock_download.assert_called_once()
    mock_process.assert_called_once()


def test_soap_lattes_remote_check_failure_falls_back_to_download(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    curr_dir.mkdir(parents=True)

    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942"/>'
    )
    old_time = time.time() - (2 * 86400)
    os.utime(xml_file, (old_time, old_time))

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=2,
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 1},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.fetch_lattes_last_update',
            return_value=None,
        ),
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip',
            return_value=b'fake_zip',
        ) as mock_download,
        patch(
            'simcc_etl.routines.soap_lattes.process_lattes_content',
            return_value=True,
        ),
    ):
        stats = run_soap(None, settings=settings)

    assert stats.succeeded == 1
    mock_download.assert_called_once()


def test_soap_lattes_force_bypasses_ttl_and_date_check(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    curr_dir.mkdir(parents=True)
    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942"/>'
    )

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=2,
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 30},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.fetch_lattes_last_update'
        ) as mock_last_update,
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip',
            return_value=b'fake_zip',
        ) as mock_download,
        patch(
            'simcc_etl.routines.soap_lattes.process_lattes_content',
            return_value=True,
        ),
    ):
        stats = run_soap(None, settings=settings, force=True)

    assert stats.succeeded == 1
    mock_last_update.assert_not_called()
    mock_download.assert_called_once()


def test_soap_lattes_check_last_update_disabled(tmp_path):
    from simcc_etl.routines.soap_lattes import run as run_soap

    curr_dir = tmp_path / 'xml' / 'current'
    curr_dir.mkdir(parents=True)
    xml_file = curr_dir / '1966167015825708.xml'
    xml_file.write_text(
        '<CURRICULO-VITAE NUMERO-IDENTIFICADOR="1966167015825708" '
        'DATA-ATUALIZACAO="14112026" HORA-ATUALIZACAO="185942"/>'
    )
    old_time = time.time() - (2 * 86400)
    os.utime(xml_file, (old_time, old_time))

    settings = Settings(
        STORAGE_DIR=tmp_path,
        ADMIN_SERVICE_TOKEN='test',
        SOAP_MAX_WORKERS=2,
        CHECK_LATTES_LAST_UPDATE=False,
    )

    with (
        patch(
            'simcc_etl.routines.soap_lattes._get_target_lattes_ids',
            return_value={'1966167015825708': 1},
        ),
        patch(
            'simcc_etl.routines.soap_lattes.fetch_lattes_last_update'
        ) as mock_last_update,
        patch(
            'simcc_etl.routines.soap_lattes.download_lattes_zip',
            return_value=b'fake_zip',
        ) as mock_download,
        patch(
            'simcc_etl.routines.soap_lattes.process_lattes_content',
            return_value=True,
        ),
    ):
        stats = run_soap(None, settings=settings)

    assert stats.succeeded == 1
    mock_last_update.assert_not_called()
    mock_download.assert_called_once()
