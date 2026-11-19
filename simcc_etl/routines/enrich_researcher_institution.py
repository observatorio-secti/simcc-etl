import csv
import re
from bisect import bisect_right
from collections.abc import Callable
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


def _enrich_from_csvs(
    session: Session,
    researchers_dir: Path,
    institution_ids: dict[str, str],
    city_ids: dict[str, str],
    cep_lookup: CepLookup,
    territories: dict[str, str],
    allowed_lattes: set[str] | None,
) -> tuple[int, int, int]:
    """Atualiza researcher_institution a partir dos CSVs oficiais das instituições."""
    if not researchers_dir.exists():
        logger.warning(
            f'Diretório de pesquisadores não encontrado: {researchers_dir}'
        )
        return 0, 0, 0

    csv_files = sorted(researchers_dir.glob('*.csv'))
    if not csv_files:
        logger.warning(f'Nenhum CSV encontrado em {researchers_dir}')
        return 0, 0, 0

    updates_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    found = 0
    failed = 0

    for csv_path in csv_files:
        acronym = csv_path.stem.strip().upper()
        institution_id = institution_ids.get(acronym)
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
            if allowed_lattes is not None and lattes_id not in allowed_lattes:
                continue

            found += 1
            workload = parse_workload(canonical.get('workload'))
            city_id, territory = _resolve_location(
                canonical, cep_lookup, territories, city_ids
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
        for i in range(0, len(params), BATCH_SIZE):
            session.execute(update_sql, params[i : i + BATCH_SIZE])

    return found, len(params), failed


def _enrich_missing_from_address(
    session: Session,
    institution_by_name: dict[str, str],
    city_ids: dict[str, str],
    cep_lookup: CepLookup,
    territories: dict[str, str],
    allowed_lattes: set[str] | None,
) -> tuple[int, int, int]:
    """Preenche researcher_institution via CEP de researcher_address para quem não tem vínculo."""
    rows = (
        session.execute(
            text(
                """
                SELECT DISTINCT ON (ra.researcher_id)
                    ra.researcher_id::TEXT AS researcher_id,
                    r.lattes_id,
                    ra.cep,
                    ra.institution AS address_institution,
                    r.institution_id::TEXT AS researcher_institution_id
                FROM public.researcher_address ra
                JOIN public.researcher r ON r.id = ra.researcher_id
                WHERE ra.cep IS NOT NULL
                  AND TRIM(ra.cep) <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM public.researcher_institution ri
                      WHERE ri.researcher_id = ra.researcher_id
                  )
                ORDER BY ra.researcher_id, ra.id;
                """
            )
        )
        .mappings()
        .all()
    )

    inserts_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    found = 0
    skipped_no_location = 0
    skipped_no_institution = 0

    for row in rows:
        lattes_id = clean_lattes_id(row.get('lattes_id'))
        if allowed_lattes is not None and (
            not lattes_id or lattes_id not in allowed_lattes
        ):
            continue

        found += 1
        cep_city = cep_lookup.resolve(row.get('cep'))
        if not cep_city:
            skipped_no_location += 1
            continue

        city_key = _normalize_city_key(cep_city)
        city_id = city_ids.get(city_key) if city_key else None
        territory = territories.get(city_key) if city_key else None
        if not city_id and not territory:
            skipped_no_location += 1
            continue

        addr_inst_key = normalize_name_match(row.get('address_institution'))
        institution_id = (
            institution_by_name.get(addr_inst_key) if addr_inst_key else None
        ) or row.get('researcher_institution_id')
        if not institution_id:
            skipped_no_institution += 1
            continue

        researcher_id = str(row['researcher_id'])
        inserts_by_key[(researcher_id, str(institution_id))] = {
            'researcher_id': researcher_id,
            'institution_id': str(institution_id),
            'identity_territory': territory,
            'city_id': city_id,
        }

    params = list(inserts_by_key.values())
    if params:
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
        for i in range(0, len(params), BATCH_SIZE):
            session.execute(insert_sql, params[i : i + BATCH_SIZE])

    logger.info(
        f'Fallback via researcher_address (CEP): {len(params)} inseridos '
        f'de {found} elegíveis ({skipped_no_location} sem localização, '
        f'{skipped_no_institution} sem institution_id)'
    )
    return found, len(params), skipped_no_location + skipped_no_institution


def run(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
    cep_ranges_path: Path = DEFAULT_CEP_RANGES_PATH,
    territories_path: Path = DEFAULT_TERRITORIES_PATH,
) -> Stats:
    """Enriquece researcher_institution (território, carga e cidade)."""
    if settings is None:
        settings = Settings()

    cep_lookup = CepLookup(cep_ranges_path)
    territories = load_identity_territories(territories_path)
    institution_by_acronym, institution_by_name = _load_institutions(session)
    city_ids = _load_city_ids(session)

    allowed_lattes = (
        {clean_lattes_id(lid) for lid in lattes_ids if clean_lattes_id(lid)}
        if lattes_ids
        else None
    )

    csv_found, csv_succeeded, csv_failed = _enrich_from_csvs(
        session=session,
        researchers_dir=settings.seed_researchers_dir,
        institution_ids=institution_by_acronym,
        city_ids=city_ids,
        cep_lookup=cep_lookup,
        territories=territories,
        allowed_lattes=allowed_lattes,
    )
    addr_found, addr_succeeded, addr_failed = _enrich_missing_from_address(
        session=session,
        institution_by_name=institution_by_name,
        city_ids=city_ids,
        cep_lookup=cep_lookup,
        territories=territories,
        allowed_lattes=allowed_lattes,
    )
    session.commit()

    total_found = csv_found + addr_found
    total_succeeded = csv_succeeded + addr_succeeded
    total_failed = csv_failed + addr_failed

    logger.info(
        f'Enriquecimento de researcher_institution concluído: '
        f'{csv_succeeded} atualizados via CSV, '
        f'{addr_succeeded} inseridos via researcher_address '
        f'({total_failed} inválidos/ignorados)'
    )
    return Stats(
        found=total_found, succeeded=total_succeeded, failed=total_failed
    )
