import concurrent.futures
import io
import json
import os
import shutil
import time
import xml.etree.ElementTree as etree
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Literal

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
CNPQ_WSDL_URL = 'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl'
_REMOTE_DATE_FORMATS = (
    '%d/%m/%Y %H:%M:%S',
    '%d%m%Y%H%M%S',
    '%d/%m/%Y %H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
)
_JSON_DATE_KEYS = (
    'data_atualizacao',
    'dataAtualizacao',
    'last_update',
    'data',
    'date',
)


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


def get_local_xml_last_update(xml_path: Path) -> datetime | None:
    """Extrai DATA-ATUALIZACAO e HORA-ATUALIZACAO da raiz do XML."""
    try:
        for _, elem in etree.iterparse(xml_path, events=('start',)):
            if elem.tag != 'CURRICULO-VITAE':
                return None
            date_str = (elem.attrib.get('DATA-ATUALIZACAO') or '').strip()
            time_str = (elem.attrib.get('HORA-ATUALIZACAO') or '').strip()
            if not date_str or not time_str:
                return None
            return datetime.strptime(f'{date_str}{time_str}', '%d%m%Y%H%M%S')
    except Exception:
        return None
    return None


def _parse_remote_lattes_date(raw: object) -> datetime | None:
    """Converte resposta de getDataAtualizacaoCV em datetime naive."""
    if isinstance(raw, datetime):
        return raw.replace(tzinfo=None)
    if not isinstance(raw, str):
        return None

    text = raw.strip()
    if not text:
        return None

    if text.startswith('{') or text.startswith('"'):
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict):
            for key in _JSON_DATE_KEYS:
                if key in decoded:
                    return _parse_remote_lattes_date(decoded[key])
            return None
        if isinstance(decoded, str):
            text = decoded.strip()

    text = text.strip('\'"').strip()
    if not text:
        return None

    for fmt in _REMOTE_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def fetch_lattes_last_update(
    lattes_id: str,
    settings: Settings,
    client: httpx.Client | None = None,
) -> datetime | None:
    """Consulta a data de última atualização do CV no CNPq/proxy."""
    try:
        if settings.ALTERNATIVE_CNPQ_SERVICE:
            url = (
                f'{settings.CNPQ_PROXY_URL}/getDataAtualizacaoCV'
                f'?lattes_id={lattes_id}'
            )
            if client is not None:
                resp = client.get(url)
                resp.raise_for_status()
                raw_date: object = resp.text
            else:
                with httpx.Client(verify=False, timeout=TIMEOUT) as c:
                    resp = c.get(url)
                    resp.raise_for_status()
                    raw_date = resp.text
        else:
            client_soap = Client(
                CNPQ_WSDL_URL,
                transport=Transport(timeout=30, operation_timeout=30),
            )
            raw_date = client_soap.service.getDataAtualizacaoCV(lattes_id)
    except Exception as e:
        logger.warning(
            f'Falha ao consultar data de atualização do Lattes {lattes_id}: '
            f'{e}',
            lattes_id=lattes_id,
            error=str(e),
        )
        return None

    parsed = _parse_remote_lattes_date(raw_date)
    if parsed is None:
        logger.warning(
            f'Data de atualização inválida para Lattes {lattes_id}',
            lattes_id=lattes_id,
            raw_date=str(raw_date)[:100],
        )
    return parsed


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
        CNPQ_WSDL_URL,
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


def _sync_single_researcher(
    lattes_id: str,
    local_updated_at: datetime | None,
    settings: Settings,
    http_client: httpx.Client,
) -> Literal['unchanged', 'downloaded', 'failed']:
    """Verifica atualização remota e baixa o currículo apenas se necessário."""
    xml_curr = settings.xml_current_path
    xml_run = settings.xml_run_path
    cached_file = xml_curr / f'{lattes_id}.xml'

    if local_updated_at is not None:
        remote_updated_at = fetch_lattes_last_update(
            lattes_id, settings, client=http_client
        )
        if (
            remote_updated_at is not None
            and remote_updated_at <= local_updated_at
        ):
            # Atualiza mtime para reiniciar o ciclo de TTL (ex.: 30 dias).
            os.utime(cached_file, None)
            shutil.copy2(cached_file, xml_run / f'{lattes_id}.xml')
            return 'unchanged'

    try:
        zip_bytes = download_lattes_zip(
            lattes_id, settings, client=http_client
        )
        if not zip_bytes:
            logger.warning(
                f'Retorno vazio para pesquisador {lattes_id}',
                lattes_id=lattes_id,
            )
            return 'failed'
        ok = process_lattes_content(lattes_id, zip_bytes, xml_curr, xml_run)
        return 'downloaded' if ok else 'failed'
    except Exception as e:
        logger.error(
            f'Falha ao baixar Lattes {lattes_id}: {e}',
            lattes_id=lattes_id,
            error=str(e),
        )
        return 'failed'


def run(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
    force: bool = False,
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

    ttl_cached = 0
    unchanged_remote = 0
    downloaded = 0
    failed = 0
    pending: list[tuple[str, datetime | None]] = []
    now = time.time()

    for lattes_id, refresh_interval_days in targets.items():
        cached_file = xml_curr / f'{lattes_id}.xml'
        has_cached = cached_file.is_file() and cached_file.stat().st_size > 0
        if not force and has_cached:
            age_days = (now - cached_file.stat().st_mtime) / 86400.0
            if age_days < refresh_interval_days:
                shutil.copy2(cached_file, xml_run / f'{lattes_id}.xml')
                ttl_cached += 1
                continue

        local_updated_at = (
            get_local_xml_last_update(cached_file)
            if (not force and has_cached and settings.CHECK_LATTES_LAST_UPDATE)
            else None
        )
        pending.append((lattes_id, local_updated_at))

    if not pending:
        logger.info(f'Todos os {len(targets)} pesquisadores no cache (TTL).')
        return Stats(found=len(targets), succeeded=ttl_cached, failed=failed)

    logger.info(
        f'{ttl_cached} em cache (TTL). Processando {len(pending)} '
        f'pesquisadores com {settings.SOAP_MAX_WORKERS} workers...'
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
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.SOAP_MAX_WORKERS
        ) as executor:
            future_to_id = {
                executor.submit(
                    _sync_single_researcher,
                    lid,
                    local_updated_at,
                    settings,
                    http_client,
                ): lid
                for lid, local_updated_at in pending
            }
            for future in concurrent.futures.as_completed(future_to_id):
                outcome = future.result()
                if outcome == 'unchanged':
                    unchanged_remote += 1
                elif outcome == 'downloaded':
                    downloaded += 1
                else:
                    failed += 1

    succeeded = ttl_cached + unchanged_remote + downloaded
    logger.info(
        'Coleta Lattes finalizada',
        found=len(targets),
        ttl_cached=ttl_cached,
        unchanged_remote=unchanged_remote,
        downloaded=downloaded,
        failed=failed,
    )
    return Stats(found=len(targets), succeeded=succeeded, failed=failed)
