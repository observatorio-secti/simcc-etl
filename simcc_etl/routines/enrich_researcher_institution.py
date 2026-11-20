import csv
import re
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings
from simcc_etl.core.text import (
    clean_cep,
    clean_lattes_id,
    normalize_name_match,
    normalize_text,
)

logger = get_logger('simcc_etl.routines.enrich_researcher_institution')

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
DEFAULT_CEP_RANGES_PATH = DATA_DIR / 'cep_ranges.csv'
DEFAULT_TERRITORIES_PATH = DATA_DIR / 'identity_territories.csv'
DEFAULT_RESEARCHERS_DIR = DATA_DIR / 'researchers'

BATCH_SIZE = 5000
WORKLOAD_MIN_HOURS = 0.0
WORKLOAD_MAX_HOURS = 168.0
EXCLUSIVE_DEDICATION_HOURS = 40.0
EXCLUSIVE_DEDICATION_TOKENS = frozenset({
    'de',
    'dedc exclus',
    'dedicacao exclusiva',
})

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    'lattes_id': ('lattes_id', 'id_lattes', 'idlattes', 'lattes'),
    'workload': (
        'ch semanal',
        'carga horaria',
        'carga_horaria',
        'jornada trabalho',
        'work_regime',
        'workload',
        'regime',
    ),
    'cep': ('cep', 'zip_code', 'zipcode', 'codigo_postal'),
    'city': ('city', 'cidade', 'municipio', 'campus_city'),
    'identity_territory': (
        'identity_territory',
        'territorio_identidade',
        'territorio de identidade',
        'territorio',
    ),
}

CITY_ALIASES: dict[str, str] = {
    'SANTOAMARODAPURIFICACAO': 'SANTOAMARO',
}

# Instituições com centro único/consolidado para fallback (Camada 4)
SINGLE_CENTER_INSTITUTIONS: dict[str, str] = {
    'EBMSP': 'Salvador',
    'FIOCRUZ-IGM': 'Salvador',
    'UEFS': 'Feira de Santana',
    'UESC': 'Ilhéus',
}

# Regras para Camada 5 (workload via researcher_professional_experience)
FORMAL_EMPLOYMENT_TYPES = frozenset({
    'Servidor_Publico',
    'Celetista',
    'Professor_Visitante',
})
RPE_NOISE_PATTERN = re.compile(
    r'(bols|estagi|aluno|discente|estudante|volunt|monitor|tutor|'
    r'revisor|editorial|comit)',
    re.IGNORECASE,
)

InstitutionParser = Callable[[dict[str, Any]], dict[str, Any]]
INSTITUTION_PARSERS: dict[str, InstitutionParser] = {}


def register_institution_parser(
    acronym: str, parser: InstitutionParser
) -> None:
    """Registra um parser customizado para uma instituição (por sigla)."""
    INSTITUTION_PARSERS[acronym.strip().upper()] = parser


class CepLookup:
    """Índice em memória para resolver CEP em município via busca binária."""

    def __init__(self, path: Path = DEFAULT_CEP_RANGES_PATH) -> None:
        if not path.exists():
            raise FileNotFoundError(
                f'Arquivo obrigatório de faixas de CEP não encontrado: {path}'
            )

        ranges: list[tuple[int, int, str, str]] = []
        with path.open(encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f):
                city = (row.get('city') or '').strip()
                uf = (row.get('uf') or '').strip()
                cep_start = (row.get('cep_start') or '').strip()
                cep_end = (row.get('cep_end') or '').strip()
                if city and cep_start.isdigit() and cep_end.isdigit():
                    ranges.append((int(cep_start), int(cep_end), uf, city))

        ranges.sort(key=lambda item: (item[0], item[1]))
        self._ranges = ranges
        self._starts = [item[0] for item in ranges]

    def resolve(self, cep: Any) -> str | None:
        """Retorna o nome do município para o CEP informado, ou None."""
        cleaned = clean_cep(cep)
        if not cleaned or not self._ranges:
            return None

        cep_int = int(cleaned)
        idx = bisect_right(self._starts, cep_int) - 1
        while idx >= 0:
            start, end, _, city = self._ranges[idx]
            if start <= cep_int <= end:
                return city
            idx -= 1
        return None


def load_identity_territories(
    path: Path = DEFAULT_TERRITORIES_PATH,
) -> dict[str, str]:
    """Carrega mapa município normalizado -> território de identidade."""
    if not path.exists():
        raise FileNotFoundError(
            f'Arquivo obrigatório de territórios não encontrado: {path}'
        )

    mapping: dict[str, str] = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            city = normalize_name_match(row.get('city'))
            territory = (row.get('territory') or '').strip()
            if city and territory:
                mapping[city] = territory
    return mapping


def parse_workload(raw: Any) -> float | None:
    """Converte carga horária/regime de trabalho para horas semanais."""
    if raw is None:
        return None
    text_val = normalize_text(raw)
    if not text_val:
        return None
    if text_val in EXCLUSIVE_DEDICATION_TOKENS:
        return EXCLUSIVE_DEDICATION_HOURS

    match = re.search(r'(-?\d+(?:[.,]\d+)?)', text_val)
    if not match:
        return None

    hours = float(match.group(1).replace(',', '.'))
    if WORKLOAD_MIN_HOURS <= hours <= WORKLOAD_MAX_HOURS:
        return hours
    logger.warning(f'Carga horária fora da faixa permitida (0..168): {raw}')
    return None


def _detect_delimiter(sample_line: str) -> str:
    return ';' if sample_line.count(';') > sample_line.count(',') else ','


def _read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    last_error: UnicodeDecodeError | None = None
    for encoding in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with csv_path.open(encoding=encoding, newline='') as f:
                first_line = f.readline()
                if not first_line:
                    return []
                delimiter = _detect_delimiter(first_line)
                f.seek(0)
                return list(csv.DictReader(f, delimiter=delimiter))
        except UnicodeDecodeError as exc:
            last_error = exc
    raise RuntimeError(
        f'Não foi possível decodificar {csv_path}: {last_error}'
    )


def _canonicalize_row(
    raw_row: dict[str, Any], acronym: str
) -> dict[str, Any]:
    normalized = {normalize_text(k): v for k, v in raw_row.items() if k}
    canonical: dict[str, Any] = {}
    for field, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                canonical[field] = normalized[alias]
                break

    parser = INSTITUTION_PARSERS.get(acronym)
    if parser is not None:
        canonical = parser(canonical)
    return canonical


def _normalize_city_key(city_name: str | None) -> str:
    key = normalize_name_match(city_name)
    return CITY_ALIASES.get(key, key)


def _resolve_location(
    canonical: dict[str, Any],
    cep_lookup: CepLookup,
    territories: dict[str, str],
    city_ids: dict[str, str],
) -> tuple[str | None, str | None]:
    raw_city = (canonical.get('city') or '').strip() or None
    cep_city = cep_lookup.resolve(canonical.get('cep'))

    city_key = _normalize_city_key(raw_city or cep_city)
    if (
        cep_city
        and city_key
        and city_key not in city_ids
        and city_key not in territories
    ):
        city_key = _normalize_city_key(cep_city)

    city_id = city_ids.get(city_key) if city_key else None
    raw_territory = (canonical.get('identity_territory') or '').strip() or None
    territory = raw_territory or (
        territories.get(city_key) if city_key else None
    )
    return city_id, territory


def _load_institutions(
    session: Session,
) -> tuple[dict[str, str], dict[str, str]]:
    """Carrega mapas de instituições por sigla (CSV) e por nome/sigla normalizados."""
    rows = (
        session.execute(
            text('SELECT id::TEXT AS id, acronym, name FROM public.institution')
        )
        .mappings()
        .all()
    )
    by_acronym: dict[str, str] = {}
    by_normalized: dict[str, str] = {}
    for row in rows:
        inst_id = str(row['id'])
        acronym = row.get('acronym')
        name = row.get('name')
        if acronym:
            by_acronym[acronym.strip().upper()] = inst_id
            norm_acronym = normalize_name_match(acronym)
            if norm_acronym:
                by_normalized.setdefault(norm_acronym, inst_id)
        if name:
            norm_name = normalize_name_match(name)
            if norm_name:
                by_normalized.setdefault(norm_name, inst_id)
    return by_acronym, by_normalized


def _load_city_ids(session: Session) -> dict[str, str]:
    rows = (
        session.execute(
            text(
                'SELECT id::TEXT AS id, name FROM public.city '
                'WHERE name IS NOT NULL'
            )
        )
        .mappings()
        .all()
    )
    return {
        normalize_name_match(row['name']): str(row['id'])
        for row in rows
        if row['name']
    }


def _collect_csv_files(
    researchers_dir: Path, extra_dir: Path | None = None
) -> list[Path]:
    csv_by_name: dict[str, Path] = {}
    if researchers_dir.exists():
        for path in sorted(researchers_dir.glob('*.csv')):
            csv_by_name[path.name] = path
    if extra_dir and extra_dir.exists():
        for path in sorted(extra_dir.glob('*.csv')):
            csv_by_name[path.name] = path

    if not csv_by_name:
        raise FileNotFoundError(
            f'Nenhum CSV de pesquisadores encontrado em {researchers_dir}'
        )
    return [csv_by_name[name] for name in sorted(csv_by_name)]


@dataclass(frozen=True)
class EnrichmentContext:
    """Contexto compartilhado entre os estágios da pipeline de enriquecimento."""

    cep_lookup: CepLookup
    territories: dict[str, str]
    city_ids: dict[str, str]
    institution_by_acronym: dict[str, str]
    institution_by_name: dict[str, str]
    allowed_lattes: set[str] | None = None


def _execute_in_batches(
    session: Session, sql: Any, params: list[dict[str, Any]]
) -> None:
    for i in range(0, len(params), BATCH_SIZE):
        session.execute(sql, params[i : i + BATCH_SIZE])


def _resolve_address_location(
    cep: Any,
    city: Any,
    uf: Any,
    ctx: EnrichmentContext,
) -> tuple[str | None, str | None]:
    """Resolve (city_id, identity_territory) via CEP (Camada 2) ou cidade+UF (Camada 3)."""
    cep_city = ctx.cep_lookup.resolve(cep)
    if cep_city:
        city_key = _normalize_city_key(cep_city)
        city_id = ctx.city_ids.get(city_key) if city_key else None
        territory = ctx.territories.get(city_key) if city_key else None
        if city_id or territory:
            return city_id, territory

    raw_city = (city or '').strip() if isinstance(city, str) else ''
    if raw_city:
        city_key = _normalize_city_key(raw_city)
        uf_norm = (uf or '').strip().upper() if isinstance(uf, str) else ''
        city_id = ctx.city_ids.get(city_key) if city_key else None
        # Evita homônimos de outros estados ao atribuir Território de Identidade da BA
        territory = (
            ctx.territories.get(city_key)
            if city_key and uf_norm in ('', 'BA')
            else None
        )
        if city_id or territory:
            return city_id, territory

    return None, None


def _stage_official_csvs(
    session: Session,
    ctx: EnrichmentContext,
    csv_files: list[Path],
) -> tuple[int, int, int]:
    """Camada 1: Atualiza researcher_institution a partir dos CSVs oficiais."""

    updates_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    found = 0
    failed = 0

    for csv_path in csv_files:
        acronym = csv_path.stem.strip().upper()
        institution_id = ctx.institution_by_acronym.get(acronym)
        if not institution_id:
            logger.warning(
                f'Instituição "{acronym}" ({csv_path.name}) não encontrada '
                'em public.institution. Pulando.'
            )
            continue

        rows = _read_csv_rows(csv_path)
        logger.info(
            f'Processando {len(rows)} registros de {csv_path.name} '
            f'(instituição {acronym})'
        )
        for raw_row in rows:
            canonical = _canonicalize_row(raw_row, acronym)
            lattes_id = clean_lattes_id(canonical.get('lattes_id'))
            if not lattes_id:
                failed += 1
                continue
            if (
                ctx.allowed_lattes is not None
                and lattes_id not in ctx.allowed_lattes
            ):
                continue

            found += 1
            workload = parse_workload(canonical.get('workload'))
            city_id, territory = _resolve_location(
                canonical, ctx.cep_lookup, ctx.territories, ctx.city_ids
            )
            updates_by_key[(lattes_id, institution_id)] = {
                'lattes_id': lattes_id,
                'institution_id': institution_id,
                'identity_territory': territory,
                'workload': workload,
                'city_id': city_id,
            }

    params = list(updates_by_key.values())
    if params:
        update_sql = text(
            """
            UPDATE public.researcher_institution ri
            SET
                identity_territory = COALESCE(
                    :identity_territory, ri.identity_territory
                ),
                workload = COALESCE(:workload, ri.workload),
                city_id = COALESCE(:city_id, ri.city_id)
            FROM public.researcher r
            WHERE ri.researcher_id = r.id
              AND r.lattes_id = :lattes_id
              AND ri.institution_id = CAST(:institution_id AS UUID);
            """
        )
        _execute_in_batches(session, update_sql, params)

    return found, len(params), failed


def _stage_address_enrichment(
    session: Session,
    ctx: EnrichmentContext,
) -> tuple[int, int, int]:
    """Camadas 2 e 3: Enriquece via CEP e cidade+UF de researcher_address."""
    rows = (
        session.execute(
            text(
                """
                WITH link_counts AS (
                    SELECT researcher_id, COUNT(*) AS cnt
                    FROM public.researcher_institution
                    GROUP BY researcher_id
                )
                SELECT DISTINCT ON (ra.researcher_id, ri.institution_id)
                    ra.researcher_id::TEXT AS researcher_id,
                    r.lattes_id,
                    ra.cep,
                    ra.city,
                    ra.uf,
                    ra.institution AS address_institution,
                    r.institution_id::TEXT AS researcher_institution_id,
                    ri.institution_id::TEXT AS existing_institution_id,
                    COALESCE(lc.cnt, 0) AS link_count
                FROM public.researcher_address ra
                JOIN public.researcher r ON r.id = ra.researcher_id
                LEFT JOIN public.researcher_institution ri
                    ON ri.researcher_id = ra.researcher_id
                LEFT JOIN link_counts lc
                    ON lc.researcher_id = ra.researcher_id
                WHERE (
                    (ra.cep IS NOT NULL AND TRIM(ra.cep) <> '')
                    OR (ra.city IS NOT NULL AND TRIM(ra.city) <> '')
                )
                  AND (
                    ri.researcher_id IS NULL
                    OR ri.identity_territory IS NULL
                    OR ri.city_id IS NULL
                  )
                ORDER BY ra.researcher_id, ri.institution_id, ra.id;
                """
            )
        )
        .mappings()
        .all()
    )

    inserts_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    updates_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    found = 0
    skipped_no_location = 0
    skipped_no_institution = 0
    skipped_multi_link = 0

    for row in rows:
        lattes_id = clean_lattes_id(row.get('lattes_id'))
        if ctx.allowed_lattes is not None and (
            not lattes_id or lattes_id not in ctx.allowed_lattes
        ):
            continue

        found += 1
        city_id, territory = _resolve_address_location(
            row.get('cep'), row.get('city'), row.get('uf'), ctx
        )
        if not city_id and not territory:
            skipped_no_location += 1
            continue

        addr_inst_key = normalize_name_match(row.get('address_institution'))
        addr_inst_id = (
            ctx.institution_by_name.get(addr_inst_key)
            if addr_inst_key
            else None
        )
        researcher_id = str(row['researcher_id'])
        existing_inst_id = row.get('existing_institution_id')

        if existing_inst_id:
            link_count = int(row.get('link_count') or 0)
            if link_count > 1 and addr_inst_id != str(existing_inst_id):
                skipped_multi_link += 1
                continue
            updates_by_key[(researcher_id, str(existing_inst_id))] = {
                'researcher_id': researcher_id,
                'institution_id': str(existing_inst_id),
                'identity_territory': territory,
                'city_id': city_id,
            }
        else:
            institution_id = addr_inst_id or row.get(
                'researcher_institution_id'
            )
            if not institution_id:
                skipped_no_institution += 1
                continue
            inserts_by_key[(researcher_id, str(institution_id))] = {
                'researcher_id': researcher_id,
                'institution_id': str(institution_id),
                'identity_territory': territory,
                'city_id': city_id,
            }

    insert_params = list(inserts_by_key.values())
    if insert_params:
        insert_sql = text(
            """
            INSERT INTO public.researcher_institution (
                researcher_id,
                institution_id,
                identity_territory,
                city_id
            )
            VALUES (
                CAST(:researcher_id AS UUID),
                CAST(:institution_id AS UUID),
                :identity_territory,
                CAST(:city_id AS UUID)
            )
            ON CONFLICT (researcher_id, institution_id) DO NOTHING;
            """
        )
        _execute_in_batches(session, insert_sql, insert_params)

    update_params = list(updates_by_key.values())
    if update_params:
        update_sql = text(
            """
            UPDATE public.researcher_institution
            SET
                identity_territory = COALESCE(
                    identity_territory, :identity_territory
                ),
                city_id = COALESCE(city_id, CAST(:city_id AS UUID))
            WHERE researcher_id = CAST(:researcher_id AS UUID)
              AND institution_id = CAST(:institution_id AS UUID);
            """
        )
        _execute_in_batches(session, update_sql, update_params)

    succeeded = len(insert_params) + len(update_params)
    failed = skipped_no_location + skipped_no_institution + skipped_multi_link
    logger.info(
        f'Camadas 2-3 (researcher_address): {len(insert_params)} inseridos, '
        f'{len(update_params)} atualizados de {found} elegíveis '
        f'({skipped_no_location} sem localização, '
        f'{skipped_no_institution} sem institution_id, '
        f'{skipped_multi_link} multi-vínculo divergente)'
    )
    return found, succeeded, failed


def _stage_institution_headquarters(
    session: Session,
    ctx: EnrichmentContext,
    headquarters: dict[str, str] = SINGLE_CENTER_INSTITUTIONS,
) -> int:
    """Camada 4: Fallback pela cidade-sede para instituições de centro único."""
    total_updated = 0
    for acronym, city_name in headquarters.items():
        institution_id = ctx.institution_by_acronym.get(acronym.upper())
        if not institution_id:
            continue

        city_key = _normalize_city_key(city_name)
        city_id = ctx.city_ids.get(city_key) if city_key else None
        territory = ctx.territories.get(city_key) if city_key else None
        if not city_id and not territory:
            continue

        if ctx.allowed_lattes is not None:
            result = session.execute(
                text(
                    """
                    UPDATE public.researcher_institution ri
                    SET
                        identity_territory = COALESCE(
                            ri.identity_territory, :identity_territory
                        ),
                        city_id = COALESCE(ri.city_id, CAST(:city_id AS UUID))
                    FROM public.researcher r
                    WHERE ri.researcher_id = r.id
                      AND ri.institution_id = CAST(:institution_id AS UUID)
                      AND (ri.identity_territory IS NULL OR ri.city_id IS NULL)
                      AND r.lattes_id = ANY(:lattes_ids);
                    """
                ),
                {
                    'institution_id': institution_id,
                    'identity_territory': territory,
                    'city_id': city_id,
                    'lattes_ids': sorted(ctx.allowed_lattes),
                },
            )
        else:
            result = session.execute(
                text(
                    """
                    UPDATE public.researcher_institution
                    SET
                        identity_territory = COALESCE(
                            identity_territory, :identity_territory
                        ),
                        city_id = COALESCE(city_id, CAST(:city_id AS UUID))
                    WHERE institution_id = CAST(:institution_id AS UUID)
                      AND (identity_territory IS NULL OR city_id IS NULL);
                    """
                ),
                {
                    'institution_id': institution_id,
                    'identity_territory': territory,
                    'city_id': city_id,
                },
            )
        rowcount = getattr(result, 'rowcount', 0)
        if isinstance(rowcount, int) and rowcount > 0:
            total_updated += rowcount

    logger.info(
        f'Camada 4 (sede institucional): {total_updated} vínculos atualizados'
    )
    return total_updated


def _resolve_rpe_workload(row: dict[str, Any]) -> float | None:
    """Extrai carga horária semanal válida de researcher_professional_experience."""
    if row.get('exclusive_dedication') is True:
        return EXCLUSIVE_DEDICATION_HOURS
    raw_hours = row.get('workload_hours_weekly')
    if raw_hours is None:
        return None
    hours = float(raw_hours)
    if WORKLOAD_MIN_HOURS <= hours <= WORKLOAD_MAX_HOURS:
        return hours
    return None


def _is_rpe_noise(row: dict[str, Any]) -> bool:
    other_emp = row.get('other_employment_type') or ''
    other_func = row.get('other_functional_classification') or ''
    combined = f'{other_emp} {other_func}'.strip()
    return bool(combined and RPE_NOISE_PATTERN.search(combined))


def _stage_rpe_workload(
    session: Session,
    ctx: EnrichmentContext,
) -> tuple[int, int, int]:
    """Camada 5: Preenche workload NULL em vínculos existentes via rpe."""
    rows = (
        session.execute(
            text(
                """
                SELECT
                    ri.researcher_id::TEXT AS researcher_id,
                    ri.institution_id::TEXT AS institution_id,
                    r.lattes_id,
                    rpe.enterprise,
                    rpe.employment_type,
                    rpe.other_employment_type,
                    rpe.other_functional_classification,
                    rpe.workload_hours_weekly,
                    rpe.exclusive_dedication,
                    rpe.start_year
                FROM public.researcher_institution ri
                JOIN public.researcher r ON r.id = ri.researcher_id
                JOIN public.researcher_professional_experience rpe
                    ON rpe.researcher_id = ri.researcher_id
                WHERE ri.workload IS NULL
                  AND rpe.end_year IS NULL
                  AND (
                      rpe.exclusive_dedication IS TRUE
                      OR (
                          rpe.workload_hours_weekly IS NOT NULL
                          AND rpe.workload_hours_weekly BETWEEN 0 AND 168
                      )
                  );
                """
            )
        )
        .mappings()
        .all()
    )

    best_by_key: dict[tuple[str, str], tuple[tuple[int, int, float, int], float]] = {}
    found = 0
    skipped_mismatch = 0
    skipped_noise = 0

    for row in rows:
        lattes_id = clean_lattes_id(row.get('lattes_id'))
        if ctx.allowed_lattes is not None and (
            not lattes_id or lattes_id not in ctx.allowed_lattes
        ):
            continue

        found += 1
        ent_key = normalize_name_match(row.get('enterprise'))
        ent_inst_id = ctx.institution_by_name.get(ent_key) if ent_key else None
        ri_inst_id = str(row['institution_id'])
        if not ent_inst_id or ent_inst_id != ri_inst_id:
            skipped_mismatch += 1
            continue

        if _is_rpe_noise(row):
            skipped_noise += 1
            continue

        workload = _resolve_rpe_workload(row)
        if workload is None:
            continue

        emp_type = row.get('employment_type') or ''
        is_formal = 1 if emp_type in FORMAL_EMPLOYMENT_TYPES else 0
        is_de = 1 if row.get('exclusive_dedication') is True else 0
        start_year = int(row.get('start_year') or 0)
        priority = (is_formal, is_de, workload, start_year)

        key = (str(row['researcher_id']), ri_inst_id)
        current = best_by_key.get(key)
        if current is None or priority > current[0]:
            best_by_key[key] = (priority, workload)

    params = [
        {
            'researcher_id': res_id,
            'institution_id': inst_id,
            'workload': wl,
        }
        for (res_id, inst_id), (_, wl) in best_by_key.items()
    ]
    if params:
        update_sql = text(
            """
            UPDATE public.researcher_institution
            SET workload = COALESCE(workload, :workload)
            WHERE researcher_id = CAST(:researcher_id AS UUID)
              AND institution_id = CAST(:institution_id AS UUID)
              AND workload IS NULL;
            """
        )
        _execute_in_batches(session, update_sql, params)

    logger.info(
        f'Camada 5 (rpe workload): {len(params)} atualizados de {found} '
        f'candidatos ({skipped_mismatch} enterprise divergente, '
        f'{skipped_noise} ruído descartado)'
    )
    return found, len(params), skipped_mismatch + skipped_noise


def run(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
    researchers_dir: Path = DEFAULT_RESEARCHERS_DIR,
) -> Stats:
    """Enriquece researcher_institution em camadas (CSV, endereço, sede e rpe)."""
    if settings is None:
        settings = Settings()

    cep_lookup = CepLookup(DEFAULT_CEP_RANGES_PATH)
    territories = load_identity_territories(DEFAULT_TERRITORIES_PATH)
    csv_files = _collect_csv_files(
        researchers_dir,
        extra_dir=settings.seed_researchers_dir,
    )
    institution_by_acronym, institution_by_name = _load_institutions(session)
    city_ids = _load_city_ids(session)

    allowed_lattes = (
        {clean_lattes_id(lid) for lid in lattes_ids if clean_lattes_id(lid)}
        if lattes_ids
        else None
    )

    ctx = EnrichmentContext(
        cep_lookup=cep_lookup,
        territories=territories,
        city_ids=city_ids,
        institution_by_acronym=institution_by_acronym,
        institution_by_name=institution_by_name,
        allowed_lattes=allowed_lattes,
    )

    csv_found, csv_succeeded, csv_failed = _stage_official_csvs(
        session, ctx, csv_files
    )
    addr_found, addr_succeeded, addr_failed = _stage_address_enrichment(
        session, ctx
    )
    hq_updated = _stage_institution_headquarters(session, ctx)
    rpe_found, rpe_succeeded, rpe_failed = _stage_rpe_workload(session, ctx)
    session.commit()

    total_found = csv_found + addr_found + rpe_found
    total_succeeded = (
        csv_succeeded + addr_succeeded + hq_updated + rpe_succeeded
    )
    total_failed = csv_failed + addr_failed + rpe_failed

    logger.info(
        f'Enriquecimento de researcher_institution concluído: '
        f'{csv_succeeded} via CSV, {addr_succeeded} via researcher_address, '
        f'{hq_updated} via sede, {rpe_succeeded} workload via rpe '
        f'({total_failed} inválidos/ignorados)'
    )
    return Stats(
        found=total_found, succeeded=total_succeeded, failed=total_failed
    )
