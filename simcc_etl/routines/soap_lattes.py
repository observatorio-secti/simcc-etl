import concurrent.futures
import io
import shutil
import time
import xml.etree.ElementTree as etree
import zipfile
from pathlib import Path

import httpx
import polars as pl
from sqlalchemy.orm import Session
from zeep import Client
from zeep.transports import Transport

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings
from simcc_etl.core.text import clean_lattes_id

logger = get_logger('simcc_etl.routines.soap_lattes')

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)


def _get_target_lattes_ids(
    session: Session | None,
    lattes_ids: list[str] | None,
    settings: Settings | None = None,
) -> dict[str, int]:
    if lattes_ids:
        cleaned = [
            clean_lattes_id(lid) for lid in lattes_ids if clean_lattes_id(lid)
        ]
        return {lid: 1 for lid in dict.fromkeys(cleaned)}

    if settings is None:
        settings = Settings()

    url = f'{settings.ADMIN_API_URL}/academic/researchers/export/parquet'
    headers = {'X-Service-Token': settings.ADMIN_SERVICE_TOKEN}
    transport = httpx.HTTPTransport(retries=3)

    try:
        with httpx.Client(transport=transport, timeout=TIMEOUT) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
    except Exception as e:
        logger.error(f'Falha ao consultar pesquisadores no simcc-admin: {e}')
        raise RuntimeError(
            f'Erro ao obter pesquisadores do simcc-admin: {e}'
        ) from e

    df = pl.read_parquet(io.BytesIO(resp.content))
    if 'lattes_id' not in df.columns:
        raise ValueError(
            "Coluna 'lattes_id' ausente no Parquet retornado pelo simcc-admin."
        )

    has_refresh = 'refresh_interval_days' in df.columns
    targets: dict[str, int] = {}
    for row in df.iter_rows(named=True):
        lid = clean_lattes_id(row.get('lattes_id'))
        if not lid:
            continue
        interval = (
            int(row['refresh_interval_days'])
            if has_refresh and row.get('refresh_interval_days') is not None
            else 1
        )
        interval = max(interval, 1)

        if lid in targets:
            targets[lid] = min(targets[lid], interval)
        else:
            targets[lid] = interval

    if not targets:
        logger.error(
            'Nenhum pesquisador com Lattes ID válido retornado '
            'pelo simcc-admin.'
        )
        raise RuntimeError(
            'A lista de pesquisadores retornada pelo simcc-admin está vazia. '
            'Abortando execução do soap_lattes para evitar falso sucesso.'
        )

    return targets


def download_lattes_zip(
    lattes_id: str,
    settings: Settings,
    client: httpx.Client | None = None,
) -> bytes | None:
    if settings.ALTERNATIVE_CNPQ_SERVICE:
        url = (
            f'{settings.CNPQ_PROXY_URL}/getCurriculoCompactado'
            f'?lattes_id={lattes_id}'
        )
        if client is not None:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.content

        with httpx.Client(verify=False, timeout=TIMEOUT) as c:
            resp = c.get(url)
            resp.raise_for_status()
            return resp.content

    client_soap = Client(
        'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl',
        transport=Transport(timeout=30, operation_timeout=30),
    )
    return client_soap.service.getCurriculoCompactado(lattes_id)


def process_lattes_content(
    lattes_id: str,
    zip_bytes: bytes,
    xml_current_dir: Path,
    xml_run_dir: Path,
) -> bool:
    """Extrai e valida o arquivo XML do zip retornado pelo CNPq."""
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            xml_names = [n for n in z.namelist() if n.lower().endswith('.xml')]
            if not xml_names:
                return False

            for name in xml_names:
                xml_data = z.read(name)
                try:
                    root = etree.fromstring(xml_data)
                    ident = root.attrib.get('NUMERO-IDENTIFICADOR')
                    if not ident:
                        continue
                except Exception:
                    continue

                target_file_current = xml_current_dir / f'{lattes_id}.xml'
                target_file_run = xml_run_dir / f'{lattes_id}.xml'

                target_file_current.write_bytes(xml_data)
                target_file_run.write_bytes(xml_data)
                return True
    except Exception as e:
        logger.error(f'Erro ao extrair zip do pesquisador {lattes_id}: {e}')

    return False


def run(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
) -> Stats:
    """Baixa currículos Lattes em XML para os pesquisadores selecionados."""
    if settings is None:
        settings = Settings()
    xml_curr = settings.xml_current_path
    xml_run = settings.xml_run_path

    xml_curr.mkdir(parents=True, exist_ok=True)
    if xml_run.exists():
        shutil.rmtree(xml_run)
    xml_run.mkdir(parents=True, exist_ok=True)

    targets = _get_target_lattes_ids(session, lattes_ids, settings=settings)
    if not targets:
        logger.info('Nenhum pesquisador encontrado para download via SOAP.')
        return Stats(found=0, succeeded=0, failed=0)

    succeeded = 0
    failed = 0
    to_download: list[str] = []
    now = time.time()

    for lattes_id, refresh_interval_days in targets.items():
        cached_file = xml_curr / f'{lattes_id}.xml'
        if cached_file.is_file() and cached_file.stat().st_size > 0:
            age_days = (now - cached_file.stat().st_mtime) / 86400.0
            if age_days < refresh_interval_days:
                shutil.copy2(cached_file, xml_run / f'{lattes_id}.xml')
                succeeded += 1
                continue

        to_download.append(lattes_id)

    if not to_download:
        logger.info(f'Todos os {len(targets)} pesquisadores no cache (TTL).')
        return Stats(found=len(targets), succeeded=succeeded, failed=failed)

    logger.info(
        f'{succeeded} em cache. Baixando {len(to_download)} pesquisadores '
        f'com {settings.SOAP_MAX_WORKERS} workers...'
    )

    transport = httpx.HTTPTransport(
        retries=3,
        limits=httpx.Limits(
            max_keepalive_connections=settings.SOAP_MAX_WORKERS,
            max_connections=settings.SOAP_MAX_WORKERS * 2,
        ),
    )

    with httpx.Client(
        transport=transport, timeout=TIMEOUT, verify=False
    ) as http_client:

        def _download_and_process(lid: str) -> bool:
            try:
                zip_bytes = download_lattes_zip(
                    lid, settings, client=http_client
                )
                if not zip_bytes:
                    logger.warning(
                        f'Retorno vazio para pesquisador {lid}',
                        lattes_id=lid,
                    )
                    return False
                return process_lattes_content(
                    lid, zip_bytes, xml_curr, xml_run
                )
            except Exception as e:
                logger.error(
                    f'Falha ao baixar Lattes {lid}: {e}',
                    lattes_id=lid,
                    error=str(e),
                )
                return False

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.SOAP_MAX_WORKERS
        ) as executor:
            future_to_id = {
                executor.submit(_download_and_process, lid): lid
                for lid in to_download
            }
            for future in concurrent.futures.as_completed(future_to_id):
                if future.result():
                    succeeded += 1
                else:
                    failed += 1

    return Stats(found=len(targets), succeeded=succeeded, failed=failed)
