import io
import shutil
import xml.etree.ElementTree as etree
import zipfile
from pathlib import Path

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session
from zeep import Client
from zeep.transports import Transport

from simcc_etl.common import Stats
from simcc_etl.core.database import get_admin_engine
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings
from simcc_etl.core.text import clean_lattes_id

logger = get_logger('simcc_etl.routines.soap_lattes')

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)


def _get_target_lattes_ids(
    session: Session, lattes_ids: list[str] | None
) -> list[str]:
    if lattes_ids:
        cleaned = [
            clean_lattes_id(lid) for lid in lattes_ids if clean_lattes_id(lid)
        ]
        return list(dict.fromkeys(cleaned))

    # Consulta pesquisadores cadastrados no admin_db
    try:
        admin_eng = get_admin_engine()
        with admin_eng.connect() as conn:
            # Tenta academic.researchers (v3/develop)
            try:
                res = conn.execute(
                    text(
                        'SELECT lattes_id FROM academic.researchers '
                        'WHERE lattes_id IS NOT NULL'
                    )
                ).scalars()
                ids = [clean_lattes_id(x) for x in res if clean_lattes_id(x)]
                if ids:
                    return list(dict.fromkeys(ids))
            except Exception:
                pass

            # Fallback para public.researcher
            try:
                res = conn.execute(
                    text(
                        'SELECT lattes_id FROM public.researcher '
                        'WHERE lattes_id IS NOT NULL'
                    )
                ).scalars()
                ids = [clean_lattes_id(x) for x in res if clean_lattes_id(x)]
                return list(dict.fromkeys(ids))
            except Exception:
                pass
    except Exception as e:
        logger.warning(
            f'Não foi possível consultar admin_db: {e}',
        )

    return []


def download_lattes_zip(lattes_id: str, settings: Settings) -> bytes | None:
    if settings.ALTERNATIVE_CNPQ_SERVICE:
        url = (
            f'{settings.CNPQ_PROXY_URL}/getCurriculoCompactado'
            f'?lattes_id={lattes_id}'
        )
        with httpx.Client(verify=False, timeout=TIMEOUT) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.content

    client = Client(
        'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl',
        transport=Transport(timeout=30, operation_timeout=30),
    )
    return client.service.getCurriculoCompactado(lattes_id)


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


def run(session: Session, *, lattes_ids: list[str] | None = None) -> Stats:
    """Baixa currículos Lattes em XML para os pesquisadores selecionados."""
    settings = Settings()
    xml_curr = settings.xml_current_path
    xml_run = settings.xml_run_path

    xml_curr.mkdir(parents=True, exist_ok=True)
    if xml_run.exists():
        shutil.rmtree(xml_run)
    xml_run.mkdir(parents=True, exist_ok=True)

    targets = _get_target_lattes_ids(session, lattes_ids)
    if not targets:
        logger.info('Nenhum pesquisador encontrado para download via SOAP.')
        return Stats(found=0, succeeded=0, failed=0)

    succeeded = 0
    failed = 0

    for idx, lattes_id in enumerate(targets, start=1):
        logger.info(
            f'[{idx}/{len(targets)}] Baixando currículo: {lattes_id}',
            lattes_id=lattes_id,
        )

        cached_file = xml_curr / f'{lattes_id}.xml'
        if cached_file.exists() and cached_file.stat().st_size > 0:
            shutil.copy2(cached_file, xml_run / f'{lattes_id}.xml')
            succeeded += 1
            continue

        try:
            zip_bytes = download_lattes_zip(lattes_id, settings)
            if not zip_bytes:
                logger.warning(
                    f'Retorno vazio para pesquisador {lattes_id}',
                    lattes_id=lattes_id,
                )
                failed += 1
                continue

            if process_lattes_content(lattes_id, zip_bytes, xml_curr, xml_run):
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(
                f'Falha ao baixar Lattes {lattes_id}: {e}',
                lattes_id=lattes_id,
                error=str(e),
            )
            failed += 1

    return Stats(found=len(targets), succeeded=succeeded, failed=failed)
