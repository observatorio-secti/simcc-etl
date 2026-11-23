import json
import re
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings
from simcc_etl.core.text import clean_lattes_id

logger = get_logger('simcc_etl.routines.import_labs')

TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)


def _clean_str(value: Any) -> str | None:
    """Remove quebras de linha e normaliza espaços; retorna None se vazio."""
    if value is None:
        return None
    cleaned = re.sub(r'\s+', ' ', str(value)).strip()
    return cleaned or None


def _fetch_labs(url: str) -> list[dict[str, Any]]:
    """Busca a lista de laboratórios em JSON na API externa."""
    with httpx.Client(timeout=TIMEOUT) as client:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()

    if not isinstance(data, list):
        raise ValueError(
            'Resposta inválida da API de laboratórios: esperado lista JSON.'
        )
    return data


def _load_labs_from_file(seed_path: Path) -> list[dict[str, Any]]:
    """Lê a lista de laboratórios de um arquivo JSON local."""
    data = json.loads(seed_path.read_text(encoding='utf-8'))
    if not isinstance(data, list):
        raise ValueError(
            f'Arquivo inválido em {seed_path}: esperado lista JSON.'
        )
    return data


def _load_labs(settings: Settings) -> list[dict[str, Any]] | None:
    """Busca laboratórios na API e faz fallback para storage/seed/labs.json."""
    seed_path = settings.seed_dir / 'labs.json'

    if settings.LABS_API_URL:
        try:
            return _fetch_labs(settings.LABS_API_URL)
        except httpx.HTTPError as exc:
            if not seed_path.exists():
                raise
            logger.warning(
                'Falha ao consultar LABS_API_URL; usando fallback local.',
                error=str(exc),
                seed_path=str(seed_path),
            )
            return _load_labs_from_file(seed_path)

    if seed_path.exists():
        logger.info(
            'LABS_API_URL não configurada; carregando arquivo local.',
            seed_path=str(seed_path),
        )
        return _load_labs_from_file(seed_path)

    logger.warning(
        'LABS_API_URL não configurada e arquivo local ausente. '
        'Pulando rotina.',
        seed_path=str(seed_path),
    )
    return None


def _get_researcher_map(session: Session) -> dict[str, str]:
    """Mapeia lattes_id (16 dígitos) para researcher.id."""
    rows = (
        session.execute(
            text(
                'SELECT id::TEXT, lattes_id FROM public.researcher '
                'WHERE lattes_id IS NOT NULL;'
            )
        )
        .mappings()
        .all()
    )
    mapping: dict[str, str] = {}
    for row in rows:
        lattes = clean_lattes_id(row['lattes_id'])
        if lattes:
            mapping[lattes] = row['id']
    return mapping


def _get_institution_map(session: Session) -> dict[str, str]:
    """Mapeia sigla da instituição (maiúsculas) para institution.id."""
    rows = (
        session.execute(
            text(
                'SELECT id::TEXT, acronym FROM public.institution '
                'WHERE acronym IS NOT NULL;'
            )
        )
        .mappings()
        .all()
    )
    return {
        row['acronym'].strip().upper(): row['id']
        for row in rows
        if row['acronym'] and row['acronym'].strip()
    }


def _get_existing_labs_map(session: Session) -> dict[str, str]:
    """Mapeia hashed_id existente em public.labs para labs.id."""
    rows = (
        session.execute(
            text(
                'SELECT id::TEXT, hashed_id FROM public.labs '
                'WHERE hashed_id IS NOT NULL;'
            )
        )
        .mappings()
        .all()
    )
    return {
        row['hashed_id'].strip(): row['id']
        for row in rows
        if row['hashed_id'] and row['hashed_id'].strip()
    }


def _prepare_lab_params(
    lab: dict[str, Any],
    researcher_map: dict[str, str],
    institution_map: dict[str, str],
) -> dict[str, Any] | None:
    """Valida e normaliza um registro de laboratório para persistência."""
    hashed_id = _clean_str(lab.get('HashedID') or lab.get('hashed_id'))
    if not hashed_id:
        logger.warning('Laboratório ignorado: HashedID ausente.')
        return None

    lattes_id = clean_lattes_id(lab.get('id_lattes'))
    researcher_id = researcher_map.get(lattes_id) if lattes_id else None

    inst_acronym = _clean_str(lab.get('instituicao'))
    institution_id = (
        institution_map.get(inst_acronym.upper()) if inst_acronym else None
    )
    if not institution_id:
        logger.debug(
            'Instituição não encontrada para laboratório',
            hashed_id=hashed_id,
            instituicao=inst_acronym,
        )
        return None

    name = _clean_str(lab.get('nome'))
    lab_type = _clean_str(lab.get('tipo'))
    if not name or not lab_type:
        logger.warning(
            'Laboratório ignorado: nome ou tipo ausente.',
            hashed_id=hashed_id,
        )
        return None

    description = _clean_str(lab.get('descricao') or lab.get('description'))

    return {
        'hashed_id': hashed_id,
        'type': lab_type,
        'location': _clean_str(lab.get('local')),
        'name': name,
        'description': description,
        'website': _clean_str(lab.get('site')),
        'activities': _clean_str(lab.get('atividades')),
        'areas': _clean_str(lab.get('areas')),
        'campus': _clean_str(lab.get('campus')),
        'institution_id': institution_id,
        'researcher_id': researcher_id,
        'responsible': _clean_str(lab.get('responsavel')),
    }


def _save_labs_to_database(
    session: Session, labs: list[dict[str, Any]]
) -> tuple[int, int]:
    """Insere ou atualiza laboratórios em public.labs por hashed_id."""
    researcher_map = _get_researcher_map(session)
    institution_map = _get_institution_map(session)
    existing_labs_map = _get_existing_labs_map(session)

    insert_sql = text("""
        INSERT INTO public.labs (
            hashed_id, type, location, name, description, website,
            activities, areas, campus, institution_id, researcher_id,
            responsible
        ) VALUES (
            :hashed_id, :type, :location, :name, :description, :website,
            :activities, :areas, :campus, :institution_id, :researcher_id,
            :responsible
        )
        RETURNING id::TEXT;
    """)

    update_sql = text("""
        UPDATE public.labs SET
            type = :type,
            location = :location,
            name = :name,
            description = :description,
            website = :website,
            activities = :activities,
            areas = :areas,
            campus = :campus,
            institution_id = :institution_id,
            researcher_id = :researcher_id,
            responsible = :responsible
        WHERE id = CAST(:id AS uuid);
    """)

    succeeded = 0
    failed = 0

    for lab in labs:
        params = _prepare_lab_params(lab, researcher_map, institution_map)
        if params is None:
            failed += 1
            continue

        hashed_id = params['hashed_id']
        try:
            with session.begin_nested():
                existing_id = existing_labs_map.get(hashed_id)
                if existing_id:
                    session.execute(
                        update_sql, {**params, 'id': existing_id}
                    )
                else:
                    new_id = session.execute(insert_sql, params).scalar()
                    if new_id:
                        existing_labs_map[hashed_id] = new_id
            succeeded += 1
        except Exception as exc:
            logger.error(
                'Erro ao persistir laboratório',
                hashed_id=hashed_id,
                error=str(exc),
            )
            failed += 1

    session.commit()
    return succeeded, failed


def run(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Importa laboratórios da LABS_API_URL ou fallback local labs.json."""
    logger.info('Iniciando rotina import_labs')
    settings = Settings()

    labs = _load_labs(settings)
    if labs is None:
        return Stats(found=0, succeeded=0, failed=0)

    if lattes_ids is not None:
        target_ids = {
            cleaned
            for lid in lattes_ids
            if (cleaned := clean_lattes_id(lid)) is not None
        }
        labs = [
            lab
            for lab in labs
            if clean_lattes_id(lab.get('id_lattes')) in target_ids
        ]

    found = len(labs)
    if found == 0:
        logger.info('Nenhum laboratório encontrado para processar.')
        return Stats(found=0, succeeded=0, failed=0)

    succeeded, failed = _save_labs_to_database(session, labs)
    logger.info(
        'Rotina import_labs concluída',
        found=found,
        succeeded=succeeded,
        failed=failed,
    )
    return Stats(found=found, succeeded=succeeded, failed=failed)
