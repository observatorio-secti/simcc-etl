import ast
from datetime import date, datetime
from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings
from simcc_etl.core.text import (
    clean_code,
    normalize_name_match,
    normalize_text,
)

logger = get_logger('simcc_etl.routines.import_capes_programs')

TARGET_YEARS = list(range(2013, 2026))
LOCATION_PARTS = 2


def get_institutions_mapping(session: Session) -> dict[str, str]:
    sql = text(
        'SELECT id::TEXT, name FROM public.institution WHERE name IS NOT NULL'
    )
    result = session.execute(sql).mappings().all()
    return {normalize_text(row['name']): str(row['id']) for row in result}


def _parse_courses(cursos_raw: Any) -> tuple[bool, str, str]:
    visible = False
    types = []
    modality = ''

    if cursos_raw is not None:
        try:
            cursos_list = ast.literal_eval(str(cursos_raw))
            for course in cursos_list:
                c_norm = {normalize_text(k): v for k, v in course.items()}
                if c_norm.get('situacao') == 'em funcionamento':
                    visible = True
                n_val = normalize_text(c_norm.get('nota', ''))
                l_val = normalize_text(c_norm.get('nivel', ''))
                modality = f'{n_val}|{l_val}'
                types.append(c_norm.get('nivel', ''))
        except Exception:
            pass

    prog_modality = (
        'PROFISSIONAL' if 'profissional' in modality else 'ACADÊMICO'
    )
    return visible, prog_modality, '/'.join(types)


def _parse_start_date(raw_start: str | None) -> date | None:
    if not raw_start:
        return None
    try:
        return datetime.strptime(raw_start, '%d/%m/%Y').date()
    except Exception:
        return None


def process_program_row(
    pg: dict, institutions_map: dict[str, str]
) -> tuple[dict | None, str | None]:
    if pg.get('situacao') != 'EM FUNCIONAMENTO':
        return None, "Ignorado: Situação não é 'EM FUNCIONAMENTO'"

    municipio_raw = str(pg.get('ies_municipio', ''))
    cs = municipio_raw.split(' - ')
    city, state = cs if len(cs) == LOCATION_PARTS else ('', '')

    ies_nome = str(pg.get('ies_nome', ''))
    institution_id = institutions_map.get(normalize_text(ies_nome))
    if not institution_id:
        return None, f'Erro: Instituição não encontrada no banco ({ies_nome})'

    visible, prog_modality, prog_type = _parse_courses(pg.get('cursos'))
    phone = ''.join(c for c in str(pg.get('ies_telefones', '')) if c.isdigit())

    return {
        'code': pg.get('codigo'),
        'name': pg.get('nome'),
        'name_en': pg.get('nome ingles'),
        'basic_area': pg.get('area basica'),
        'cooperation_project': pg.get('projetos_coop'),
        'area': pg.get('area de avaliacao'),
        'modality': prog_modality,
        'type': prog_type,
        'institution_id': institution_id,
        'state': state,
        'city': city,
        'visible': visible,
        'site': pg.get('ies_url'),
        'coordinator': pg.get('coordenador'),
        'email': pg.get('ies_email'),
        'start': _parse_start_date(pg.get('ies_inicio')),
        'phone': phone,
        'periodicity': '',
    }, None


def run_programs(
    session: Session, *, lattes_ids: list[str] | None = None
) -> Stats:
    """Importa programas de pós-graduação a partir de programs.csv."""
    seed_file = Settings().seed_dir / 'programs.csv'
    if not seed_file.exists():
        logger.warning(f'Arquivo {seed_file} não encontrado. Pulando.')
        return Stats(found=0, succeeded=0, failed=0)

    df = pl.read_csv(seed_file)
    df = df.rename({col: normalize_text(col) for col in df.columns})

    institutions_map = get_institutions_mapping(session)
    valid_programs = []
    failed_count = 0

    for row in df.to_dicts():
        data, _ = process_program_row(row, institutions_map)
        if data:
            valid_programs.append(data)
        else:
            failed_count += 1

    if valid_programs:
        query = text("""
            INSERT INTO public.graduate_program (
                code, name, name_en, basic_area, cooperation_project, area,
                modality, type, institution_id, state, city, visible, site,
                coordinator, email, "start", phone, periodicity
            ) VALUES (
                :code, :name, :name_en, :basic_area, :cooperation_project,
                :area, :modality, :type, :institution_id, :state, :city,
                :visible, :site, :coordinator, :email, :start, :phone,
                :periodicity
            ) ON CONFLICT (code) DO UPDATE SET
                name = COALESCE(public.graduate_program.name,
                                EXCLUDED.name),
                name_en = COALESCE(public.graduate_program.name_en,
                                   EXCLUDED.name_en),
                basic_area = COALESCE(public.graduate_program.basic_area,
                                      EXCLUDED.basic_area),
                cooperation_project = COALESCE(
                    public.graduate_program.cooperation_project,
                    EXCLUDED.cooperation_project
                ),
                area = COALESCE(public.graduate_program.area,
                                EXCLUDED.area),
                modality = COALESCE(public.graduate_program.modality,
                                    EXCLUDED.modality),
                type = COALESCE(public.graduate_program.type,
                                EXCLUDED.type),
                institution_id = COALESCE(
                    public.graduate_program.institution_id,
                    EXCLUDED.institution_id
                ),
                state = COALESCE(public.graduate_program.state,
                                 EXCLUDED.state),
                city = COALESCE(public.graduate_program.city,
                                EXCLUDED.city),
                visible = COALESCE(public.graduate_program.visible,
                                   EXCLUDED.visible),
                site = COALESCE(public.graduate_program.site,
                                EXCLUDED.site),
                coordinator = COALESCE(public.graduate_program.coordinator,
                                       EXCLUDED.coordinator),
                email = COALESCE(public.graduate_program.email,
                                 EXCLUDED.email),
                "start" = COALESCE(public.graduate_program."start",
                                   EXCLUDED."start"),
                phone = COALESCE(public.graduate_program.phone,
                                 EXCLUDED.phone),
                periodicity = COALESCE(public.graduate_program.periodicity,
                                       EXCLUDED.periodicity);
        """)
        session.execute(query, valid_programs)
        session.commit()

    return Stats(
        found=len(df),
        succeeded=len(valid_programs),
        failed=failed_count,
    )


def run_ratings(
    session: Session, *, lattes_ids: list[str] | None = None
) -> Stats:
    """Atualiza notas CAPES dos programas de pós-graduação."""
    seed_file = Settings().seed_dir / 'rating_programs.csv'
    if not seed_file.exists():
        logger.warning(f'Arquivo {seed_file} não encontrado. Pulando.')
        return Stats(found=0, succeeded=0, failed=0)

    df = pl.read_csv(seed_file)
    df = df.rename({col: normalize_text(col) for col in df.columns})

    valid_ratings = []
    for row in df.to_dicts():
        code = clean_code(row.get('code'))
        rating = str(row.get('rating', '')).strip() or 'Não informado'
        if code:
            valid_ratings.append({'code': code, 'rating': rating})

    if valid_ratings:
        query = text("""
            UPDATE public.graduate_program
            SET rating = :rating
            WHERE code = :code;
        """)
        session.execute(query, valid_ratings)
        session.commit()

    return Stats(
        found=len(df),
        succeeded=len(valid_ratings),
        failed=len(df) - len(valid_ratings),
    )


def run_researchers(
    session: Session, *, lattes_ids: list[str] | None = None
) -> Stats:
    """Vincula pesquisadores aos programas de pós-graduação."""
    seed_file = Settings().seed_dir / 'program_researchers.csv'
    if not seed_file.exists():
        logger.warning(f'Arquivo {seed_file} não encontrado. Pulando.')
        return Stats(found=0, succeeded=0, failed=0)

    df = pl.read_csv(seed_file)
    df = df.rename({col: normalize_text(col) for col in df.columns})

    res_rows = (
        session.execute(text('SELECT id::TEXT, name FROM public.researcher'))
        .mappings()
        .all()
    )
    res_map = {normalize_name_match(r['name']): str(r['id']) for r in res_rows}

    prog_rows = (
        session.execute(
            text('SELECT id::TEXT, code FROM public.graduate_program')
        )
        .mappings()
        .all()
    )
    prog_map = {clean_code(p['code']): str(p['id']) for p in prog_rows}

    records_to_insert = []
    succeeded = 0
    failed = 0

    for row in df.to_dicts():
        name_match = normalize_name_match(row.get('nome'))
        pg_code = clean_code(row.get('id do programa'))
        categoria = (
            'PERMANENTE'
            if 'PERMANENTE' in str(row.get('categoria', '')).upper()
            else 'COLABORADOR'
        )

        r_id = res_map.get(name_match)
        pg_id = prog_map.get(pg_code)

        if not r_id or not pg_id:
            failed += 1
            continue

        succeeded += 1
        for y in TARGET_YEARS:
            records_to_insert.append({
                'graduate_program_id': pg_id,
                'researcher_id': r_id,
                'year': y,
                'type_': categoria,
            })

    if records_to_insert:
        query = text("""
            INSERT INTO public.graduate_program_researcher
                (graduate_program_id, researcher_id, year, type_)
            VALUES
                (:graduate_program_id, :researcher_id, :year, :type_)
            ON CONFLICT (graduate_program_id, researcher_id, year)
            DO UPDATE SET
                type_ = EXCLUDED.type_;
        """)
        session.execute(query, records_to_insert)
        session.commit()

    return Stats(found=len(df), succeeded=succeeded, failed=failed)
