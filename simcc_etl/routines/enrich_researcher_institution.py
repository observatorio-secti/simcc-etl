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


def _load_institution_ids(session: Session) -> dict[str, str]:
    rows = (
        session.execute(
            text(
                'SELECT id::TEXT AS id, acronym FROM public.institution '
                'WHERE acronym IS NOT NULL'
            )
        )
        .mappings()
        .all()
    )
    return {
        row['acronym'].strip().upper(): str(row['id'])
        for row in rows
        if row['acronym']
    }


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


def run(
    session: Session,
    *,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
    researchers_dir: Path = DEFAULT_RESEARCHERS_DIR,
) -> Stats:
    """Enriquece researcher_institution (território, carga e cidade)."""
    if settings is None:
        settings = Settings()

    cep_lookup = CepLookup(DEFAULT_CEP_RANGES_PATH)
    territories = load_identity_territories(DEFAULT_TERRITORIES_PATH)
    csv_files = _collect_csv_files(
        researchers_dir,
        extra_dir=settings.seed_researchers_dir,
    )

    institution_ids = _load_institution_ids(session)
    city_ids = _load_city_ids(session)

    allowed_lattes = (
        {clean_lattes_id(lid) for lid in lattes_ids if clean_lattes_id(lid)}
        if lattes_ids
        else None
    )

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
    session.commit()

    logger.info(
        f'Enriquecimento de researcher_institution concluído: '
        f'{len(params)} registros atualizados ({failed} inválidos)'
    )
    return Stats(found=found, succeeded=len(params), failed=failed)
