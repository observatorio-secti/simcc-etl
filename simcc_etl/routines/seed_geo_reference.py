"""Referência geográfica: territórios de identidade, municípios do IBGE e
código IBGE das cidades.

Popula `identity_territory` (código SEI 1 a 27) e `municipality` (código
IBGE de 7 dígitos, UF e território) a partir dos CSVs versionados em
`simcc_etl/data/`, e preenche `city.ibge_code` das cidades ainda sem código.
Não toca em pesquisador: a atribuição de cidade e território a vínculos
continua em `enrich_researcher_institution`.

Idempotente: upsert nas tabelas de referência; `city.ibge_code` só é
preenchido quando nulo, para preservar correções manuais.
"""

import csv
import re
from collections import defaultdict
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.logging import get_logger
from simcc_etl.core.text import normalize_name_match, strip_accents

logger = get_logger('simcc_etl.routines.seed_geo_reference')

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
DEFAULT_TERRITORIES_PATH = DATA_DIR / 'identity_territories.csv'
DEFAULT_MUNICIPALITIES_PATH = DATA_DIR / 'municipalities.csv'

# UF preferida quando o nome da cidade existe em mais de um estado: a
# plataforma é da Bahia e `city` não guarda UF.
PREFERRED_UF = 'BA'


def _slug(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', strip_accents(name).lower()).strip('-')


def load_territories(
    path: Path = DEFAULT_TERRITORIES_PATH,
) -> tuple[dict[int, str], dict[str, int]]:
    """Retorna (código → nome do território, IBGE → código do território)."""
    if not path.exists():
        raise FileNotFoundError(f'Arquivo de territórios ausente: {path}')
    names: dict[int, str] = {}
    by_ibge: dict[str, int] = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            code = int(row['territory_id'])
            names[code] = row['territory'].strip()
            ibge = (row.get('ibge_code') or '').strip()
            if ibge:
                by_ibge[ibge] = code
    return names, by_ibge


def load_municipalities(
    path: Path = DEFAULT_MUNICIPALITIES_PATH,
) -> list[dict[str, str]]:
    """Municípios do IBGE: `ibge_code`, `name`, `uf`."""
    if not path.exists():
        raise FileNotFoundError(f'Arquivo de municípios ausente: {path}')
    with path.open(encoding='utf-8-sig', newline='') as f:
        return [
            {k: (v or '').strip() for k, v in row.items()}
            for row in csv.DictReader(f)
        ]


def match_city_codes(
    cities: list[tuple[str, str]], municipalities: list[dict[str, str]]
) -> tuple[dict[str, str], int]:
    """Casa `(city_id, nome)` com o código IBGE pelo nome normalizado.

    Nome único entre os municípios: casa direto. Nome repetido em estados
    diferentes: casa só se houver exatamente um candidato da UF preferida.
    Retorna (city_id → IBGE, quantidade de nomes ambíguos sem casamento).
    """
    candidates: dict[str, list[dict[str, str]]] = defaultdict(list)
    for m in municipalities:
        candidates[normalize_name_match(m['name'])].append(m)

    matched: dict[str, str] = {}
    ambiguous = 0
    for city_id, name in cities:
        options = candidates.get(normalize_name_match(name), [])
        if len(options) == 1:
            matched[city_id] = options[0]['ibge_code']
            continue
        preferred = [m for m in options if m['uf'] == PREFERRED_UF]
        if len(preferred) == 1:
            matched[city_id] = preferred[0]['ibge_code']
        elif options:
            ambiguous += 1
    return matched, ambiguous


def run(
    session: Session,
    researcher_ids: list[str] | None = None,
    lattes_ids: list[str] | None = None,
) -> Stats:
    """Popula a referência geográfica e preenche `city.ibge_code`."""
    territory_names, territory_by_ibge = load_territories()
    municipalities = load_municipalities()

    session.execute(
        text("""
            INSERT INTO public.identity_territory (code, name, slug)
            VALUES (:code, :name, :slug)
            ON CONFLICT (code) DO UPDATE
               SET name = EXCLUDED.name, slug = EXCLUDED.slug;
        """),
        [
            {'code': code, 'name': name, 'slug': _slug(name)}
            for code, name in sorted(territory_names.items())
        ],
    )
    session.execute(
        text("""
            INSERT INTO public.municipality
                (ibge_code, name, uf, territory_code)
            VALUES (:ibge_code, :name, :uf, :territory_code)
            ON CONFLICT (ibge_code) DO UPDATE
               SET name = EXCLUDED.name,
                   uf = EXCLUDED.uf,
                   territory_code = EXCLUDED.territory_code;
        """),
        [
            {
                **m,
                'territory_code': territory_by_ibge.get(m['ibge_code']),
            }
            for m in municipalities
        ],
    )

    cities = [
        (row.id, row.name)
        for row in session.execute(
            text(
                'SELECT id::TEXT AS id, name FROM public.city '
                'WHERE ibge_code IS NULL'
            )
        )
    ]
    matched, ambiguous = match_city_codes(cities, municipalities)
    if matched:
        session.execute(
            text(
                'UPDATE public.city SET ibge_code = :ibge_code '
                'WHERE id = CAST(:id AS UUID) AND ibge_code IS NULL'
            ),
            [{'id': k, 'ibge_code': v} for k, v in matched.items()],
        )
    session.commit()

    failed = len(cities) - len(matched)
    logger.info(
        'Referência geográfica atualizada',
        territories=len(territory_names),
        municipalities=len(municipalities),
        cities_pending=len(cities),
        cities_matched=len(matched),
        cities_ambiguous=ambiguous,
        cities_unmatched=failed,
    )
    return Stats(found=len(cities), succeeded=len(matched), failed=failed)
