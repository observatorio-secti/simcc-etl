from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.database import ConsistencyReport, get_write_session
from simcc_etl.core.logging import get_logger
from simcc_etl.core.settings import Settings

logger = get_logger('simcc_etl.core.consistency')

STATUS_COMPLETED = 'COMPLETED'
STATUS_PARTIAL = 'PARTIAL'
STATUS_IN_PROGRESS = 'IN_PROGRESS'
STATUS_FAILED = 'FAILED'

COVERAGE_FULL = 'FULL'
COVERAGE_PARTIAL = 'PARTIAL'
COVERAGE_NA = 'NA'

EXPORT_STATUS_MAP: dict[str, str] = {
    STATUS_IN_PROGRESS: 'EM_ANDAMENTO',
    STATUS_COMPLETED: 'CONCLUIDO',
    STATUS_PARTIAL: 'CONCLUIDO_PARCIAL',
    STATUS_FAILED: 'FALHA',
}


@dataclass(frozen=True)
class ConsistencyTypeMeta:
    """Metadata describing a tracked consistency domain or routine."""

    type: str
    label: str
    category: str
    supports_manual: bool = False
    requires_full_sweep: bool = False


CONSISTENCY_CATALOG: dict[str, ConsistencyTypeMeta] = {
    'soap_lattes': ConsistencyTypeMeta(
        type='soap_lattes',
        label='Coleta SOAP Lattes',
        category='collection',
    ),
    'hop_seed': ConsistencyTypeMeta(
        type='hop_seed',
        label='Carga Inicial (Apache Hop)',
        category='seeding',
    ),
    'hop_index': ConsistencyTypeMeta(
        type='hop_index',
        label='Extração Estruturada (Apache Hop)',
        category='extraction',
    ),
    'qualis': ConsistencyTypeMeta(
        type='qualis',
        label='Tabela Qualis',
        category='reference',
        supports_manual=True,
    ),
    'foment_scholars': ConsistencyTypeMeta(
        type='foment_scholars',
        label='Bolsistas de Produtividade',
        category='reference',
        supports_manual=True,
    ),
    'capes_scraping': ConsistencyTypeMeta(
        type='capes_scraping',
        label='Raspagem Pós-Graduação (CAPES)',
        category='graduate_programs',
        supports_manual=True,
    ),
    'capes_programs': ConsistencyTypeMeta(
        type='capes_programs',
        label='Importação de Programas CAPES',
        category='graduate_programs',
    ),
    'capes_ratings': ConsistencyTypeMeta(
        type='capes_ratings',
        label='Importação de Conceitos CAPES',
        category='graduate_programs',
    ),
    'capes_researchers': ConsistencyTypeMeta(
        type='capes_researchers',
        label='Importação de Docentes CAPES',
        category='graduate_programs',
    ),
    'capes_research_lines': ConsistencyTypeMeta(
        type='capes_research_lines',
        label='Linhas de Pesquisa dos Programas',
        category='graduate_programs',
    ),
    'sanitize_lattes': ConsistencyTypeMeta(
        type='sanitize_lattes',
        label='Higienização dos Dados Lattes',
        category='sanitization',
    ),
    'remove_researchers': ConsistencyTypeMeta(
        type='remove_researchers',
        label='Remoção de Pesquisadores',
        category='sanitization',
    ),
    'enrich_researcher_institution': ConsistencyTypeMeta(
        type='enrich_researcher_institution',
        label='Vínculos Institucionais e Território',
        category='enrichment',
    ),
    'aggregate_production': ConsistencyTypeMeta(
        type='aggregate_production',
        label='Consolidação da Produção',
        category='consolidation',
    ),
    'oda_research_groups': ConsistencyTypeMeta(
        type='oda_research_groups',
        label='Grupos de Pesquisa (ODA)',
        category='consolidation',
    ),
    'indprod_researchers': ConsistencyTypeMeta(
        type='indprod_researchers',
        label='Índice de Produção - Pesquisadores',
        category='indicators',
    ),
    'indprod_graduate_programs': ConsistencyTypeMeta(
        type='indprod_graduate_programs',
        label='Índice de Produção - Pós-Graduação',
        category='indicators',
    ),
    'indprod_research_groups': ConsistencyTypeMeta(
        type='indprod_research_groups',
        label='Índice de Produção - Grupos de Pesquisa',
        category='indicators',
    ),
    'classify_researchers': ConsistencyTypeMeta(
        type='classify_researchers',
        label='Classificação de Pesquisadores',
        category='consolidation',
    ),
    'classify_article_themes': ConsistencyTypeMeta(
        type='classify_article_themes',
        label='Classificação Temática de Artigos',
        category='enrichment',
    ),
    'search_dictionary': ConsistencyTypeMeta(
        type='search_dictionary',
        label='Dicionário de Busca',
        category='search',
    ),
    'resolve_cnpq_lattes_id': ConsistencyTypeMeta(
        type='resolve_cnpq_lattes_id',
        label='Resolução de Identificador Lattes10',
        category='sanitization',
    ),
    'openalex': ConsistencyTypeMeta(
        type='openalex',
        label='Enriquecimento OpenAlex',
        category='enrichment',
        requires_full_sweep=True,
    ),
    'publish_to_read_db': ConsistencyTypeMeta(
        type='publish_to_read_db',
        label='Publicação no Banco de Leitura',
        category='publication',
    ),
}

ROUTINE_TO_CONSISTENCY_TYPES: dict[str, tuple[str, ...]] = {
    'soap_lattes': ('soap_lattes',),
    'hop_seed': ('hop_seed',),
    'hop_index': ('hop_index',),
    'import_capes_programs': ('capes_programs',),
    'import_capes_ratings': ('capes_ratings',),
    'import_capes_researchers': ('capes_researchers',),
    'import_program_research_lines': ('capes_research_lines',),
    'sanitize_lattes_data': ('sanitize_lattes',),
    'remove_researchers': ('remove_researchers',),
    'enrich_researcher_institution': ('enrich_researcher_institution',),
    'aggregate_researcher_production': ('aggregate_production',),
    'export_oda_research_groups': ('oda_research_groups',),
    'calculate_researcher_indprod': ('indprod_researchers',),
    'calculate_graduate_program_indprod': ('indprod_graduate_programs',),
    'calculate_research_group_indprod': ('indprod_research_groups',),
    'calculate_production_index': (
        'indprod_researchers',
        'indprod_graduate_programs',
        'indprod_research_groups',
    ),
    'indprod': (
        'indprod_researchers',
        'indprod_graduate_programs',
        'indprod_research_groups',
    ),
    'classify_researchers': ('classify_researchers',),
    'classify_article_themes': ('classify_article_themes',),
    'build_search_dictionary': ('search_dictionary',),
    'resolve_cnpq_lattes_id': ('resolve_cnpq_lattes_id',),
    'enrich_from_openalex': ('openalex',),
    'publish_to_read_db': ('publish_to_read_db',),
    'refresh_search_views': ('publish_to_read_db',),
}

_COUNT_PROBES: dict[str, tuple[tuple[str, str], ...]] = {
    'hop_seed': (
        ('countries', 'SELECT COUNT(*) FROM country'),
        ('states', 'SELECT COUNT(*) FROM state'),
        ('periodical_magazines', 'SELECT COUNT(*) FROM periodical_magazine'),
    ),
    'hop_index': (
        ('researchers', 'SELECT COUNT(*) FROM researcher'),
        (
            'bibliographic_productions',
            'SELECT COUNT(*) FROM bibliographic_production',
        ),
        ('foment_rows', 'SELECT COUNT(*) FROM foment'),
    ),
    'qualis': (
        ('periodical_magazines', 'SELECT COUNT(*) FROM periodical_magazine'),
        (
            'articles_with_qualis',
            'SELECT COUNT(*) FROM bibliographic_production_article '
            "WHERE qualis IS NOT NULL AND TRIM(qualis) != ''",
        ),
    ),
    'foment_scholars': (('foment_rows', 'SELECT COUNT(*) FROM foment'),),
    'capes_programs': (
        ('graduate_programs', 'SELECT COUNT(*) FROM graduate_program'),
    ),
    'capes_ratings': (
        ('graduate_programs', 'SELECT COUNT(*) FROM graduate_program'),
    ),
    'capes_researchers': (
        (
            'graduate_program_researchers',
            'SELECT COUNT(*) FROM graduate_program_researcher',
        ),
    ),
    'capes_research_lines': (
        (
            'graduate_program_research_lines',
            'SELECT COUNT(*) FROM graduate_program_research_line',
        ),
    ),
    'aggregate_production': (
        (
            'researcher_productions',
            'SELECT COUNT(*) FROM researcher_production',
        ),
    ),
    'oda_research_groups': (
        ('research_groups', 'SELECT COUNT(*) FROM research_group'),
    ),
    'indprod_researchers': (
        ('researcher_ind_prod', 'SELECT COUNT(*) FROM researcher_ind_prod'),
    ),
    'indprod_graduate_programs': (
        (
            'graduate_program_ind_prod',
            'SELECT COUNT(*) FROM graduate_program_ind_prod',
        ),
    ),
    'indprod_research_groups': (
        (
            'research_group_ind_prod',
            'SELECT COUNT(*) FROM research_group_ind_prod',
        ),
    ),
    'search_dictionary': (
        ('search_dictionary', 'SELECT COUNT(*) FROM search_dictionary'),
    ),
}


def _safe_scalar(session: Session, query: str) -> int | None:
    try:
        with session.begin_nested():
            value = session.execute(text(query)).scalar()
        return int(value) if value is not None else 0
    except SQLAlchemyError:
        return None


def _safe_id_set(session: Session, query: str) -> set[str] | None:
    try:
        with session.begin_nested():
            rows = session.execute(text(query)).scalars().all()
        return {str(row) for row in rows}
    except SQLAlchemyError:
        return None


def _known_not_found_ids(directory: Path) -> set[str]:
    if not directory.is_dir():
        return set()
    return {
        entry.stem
        for entry in directory.iterdir()
        if entry.is_file() and entry.suffix == '.json'
    }


def _probe_openalex(
    session: Session, settings: Settings | None = None
) -> dict[str, Any]:
    cfg = settings or Settings()
    not_found_base = cfg.STORAGE_DIR / 'openalex' / 'not_found'
    art_404 = _known_not_found_ids(not_found_base / 'article')
    res_404 = _known_not_found_ids(not_found_base / 'researcher')

    pending_art_ids = _safe_id_set(
        session,
        'SELECT bp.id::TEXT '
        'FROM bibliographic_production bp '
        'LEFT JOIN openalex_article oa ON oa.article_id = bp.id '
        "WHERE bp.doi IS NOT NULL AND TRIM(bp.doi) != '' "
        'AND oa.article_id IS NULL',
    )
    pending_res_ids = _safe_id_set(
        session,
        'SELECT r.id::TEXT '
        'FROM researcher r '
        'LEFT JOIN openalex_researcher oar ON oar.researcher_id = r.id '
        "WHERE r.orcid IS NOT NULL AND TRIM(r.orcid) != '' "
        'AND oar.researcher_id IS NULL',
    )
    enriched_articles = _safe_scalar(
        session, 'SELECT COUNT(*) FROM openalex_article'
    )
    enriched_researchers = _safe_scalar(
        session, 'SELECT COUNT(*) FROM openalex_researcher'
    )

    if pending_art_ids is None or pending_res_ids is None:
        return {
            'probe_unavailable': True,
            'cached_not_found_articles': len(art_404),
            'cached_not_found_researchers': len(res_404),
        }

    unseen_articles = len(pending_art_ids - art_404)
    unseen_researchers = len(pending_res_ids - res_404)
    return {
        'pending_articles_db': len(pending_art_ids),
        'pending_researchers_db': len(pending_res_ids),
        'unseen_pending_articles': unseen_articles,
        'unseen_pending_researchers': unseen_researchers,
        'cached_not_found_articles': len(art_404),
        'cached_not_found_researchers': len(res_404),
        'enriched_articles': enriched_articles,
        'enriched_researchers': enriched_researchers,
    }


def capture_snapshot(
    session: Session,
    report_type: str,
    *,
    lattes_ids: list[str] | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Captures a lightweight database/storage state snapshot for a type."""
    snapshot: dict[str, Any] = {}
    if lattes_ids:
        snapshot['lattes_ids_filter_count'] = len(lattes_ids)

    if report_type == 'openalex':
        snapshot.update(_probe_openalex(session, settings=settings))
        return snapshot

    probes = _COUNT_PROBES.get(report_type, ())
    unavailable = 0
    for key, query in probes:
        val = _safe_scalar(session, query)
        if val is None:
            unavailable += 1
        else:
            snapshot[key] = val

    if probes and unavailable == len(probes):
        snapshot['probe_unavailable'] = True

    return snapshot


def compute_deltas(
    before: dict[str, Any], after: dict[str, Any]
) -> dict[str, int]:
    """Computes numeric differences between before and after snapshots."""
    deltas: dict[str, int] = {}
    for key, before_val in before.items():
        after_val = after.get(key)
        if (
            isinstance(before_val, int)
            and not isinstance(before_val, bool)
            and isinstance(after_val, int)
            and not isinstance(after_val, bool)
        ):
            deltas[f'delta_{key}'] = after_val - before_val
    return deltas


def resolve_completion(
    report_type: str,
    stats: Stats | None,
    *,
    lattes_ids: list[str] | None = None,
    after_snapshot: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Determines (status, coverage) for partial and full-sweep rules."""
    if lattes_ids:
        return STATUS_PARTIAL, COVERAGE_PARTIAL

    if report_type == 'openalex':
        after = after_snapshot or {}
        if 'unseen_pending_articles' in after:
            remaining = after['unseen_pending_articles'] + after.get(
                'unseen_pending_researchers', 0
            )
            if remaining == 0:
                return STATUS_COMPLETED, COVERAGE_FULL
            return STATUS_PARTIAL, COVERAGE_PARTIAL
        if stats and stats.failed > 0:
            return STATUS_PARTIAL, COVERAGE_PARTIAL
        return STATUS_COMPLETED, COVERAGE_FULL

    if stats and stats.failed > 0:
        return STATUS_PARTIAL, COVERAGE_PARTIAL

    return STATUS_COMPLETED, COVERAGE_FULL


def consistency_types_for_routine(routine_name: str) -> tuple[str, ...]:
    """Maps a pipeline routine name to its consistency type keys."""
    return ROUTINE_TO_CONSISTENCY_TYPES.get(routine_name, (routine_name,))


@dataclass(frozen=True)
class RoutineExecutionMeta:
    """Context passed when tracking consistency for a pipeline routine."""

    routine_name: str
    etl_run_id: UUID | None = None
    origin: str = 'system'
    lattes_ids: list[str] | None = None
    settings: Settings | None = None


def start_consistency_reports(
    engine: Engine, meta: RoutineExecutionMeta
) -> list[str]:
    """Creates IN_PROGRESS consistency records before a routine executes."""
    report_types = consistency_types_for_routine(meta.routine_name)
    report_ids: list[str] = []
    coverage = COVERAGE_PARTIAL if meta.lattes_ids else COVERAGE_FULL

    session = get_write_session(engine)
    try:
        for report_type in report_types:
            before = capture_snapshot(
                session,
                report_type,
                lattes_ids=meta.lattes_ids,
                settings=meta.settings,
            )
            report = ConsistencyReport(
                id=str(uuid4()),
                etl_run_id=meta.etl_run_id,
                scope='simcc',
                type=report_type,
                origin=meta.origin,
                coverage=coverage,
                status=STATUS_IN_PROGRESS,
                started_at=datetime.now(timezone.utc),
                before=before,
                result={'routine': meta.routine_name},
            )
            session.add(report)
            report_ids.append(report.id)
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.warning(
            f'Could not start consistency report for {meta.routine_name}: '
            f'{exc}'
        )
        return []
    finally:
        session.close()

    return report_ids


@dataclass(frozen=True)
class RoutineOutcome:
    """Outcome details passed when finalizing consistency reports."""

    stats: Stats | None = None
    duration_ms: int = 0
    error: str | None = None


def finish_consistency_reports(
    engine: Engine,
    report_ids: list[str],
    meta: RoutineExecutionMeta,
    outcome: RoutineOutcome,
) -> None:
    """Finalizes consistency records after a routine finishes or fails."""
    if not report_ids:
        return

    session = get_write_session(engine)
    try:
        reports = session.scalars(
            select(ConsistencyReport).where(
                ConsistencyReport.id.in_(report_ids)
            )
        ).all()
        now = datetime.now(timezone.utc)
        for report in reports:
            after = capture_snapshot(
                session,
                report.type,
                lattes_ids=meta.lattes_ids,
                settings=meta.settings,
            )
            deltas = compute_deltas(report.before or {}, after)
            result_payload: dict[str, Any] = {
                **(report.result or {}),
                'routine': meta.routine_name,
                'duration_ms': outcome.duration_ms,
                **deltas,
            }
            if outcome.stats is not None:
                result_payload['stats'] = {
                    'found': outcome.stats.found,
                    'succeeded': outcome.stats.succeeded,
                    'failed': outcome.stats.failed,
                }

            if outcome.error is not None:
                report.status = STATUS_FAILED
                report.coverage = (
                    COVERAGE_PARTIAL if meta.lattes_ids else COVERAGE_NA
                )
                report.error = outcome.error
            else:
                status, coverage = resolve_completion(
                    report.type,
                    outcome.stats,
                    lattes_ids=meta.lattes_ids,
                    after_snapshot=after,
                )
                report.status = status
                report.coverage = coverage
                report.error = None

            report.after = after
            report.result = result_payload
            report.ended_at = now
            if report.status == STATUS_COMPLETED and not report.reference_at:
                report.reference_at = now

        session.commit()
    except Exception as exc:
        session.rollback()
        logger.warning(
            f'Could not finish consistency report for {meta.routine_name}: '
            f'{exc}'
        )
    finally:
        session.close()


def _compute_health(
    last_attempt: ConsistencyReport | None,
    last_complete: ConsistencyReport | None,
) -> str:
    if last_attempt is None:
        return 'PENDING'
    if last_attempt.status == STATUS_FAILED:
        return 'CRITICAL'
    if (
        last_attempt.status == STATUS_COMPLETED
        and last_attempt.coverage == COVERAGE_FULL
    ):
        return 'OK'
    if last_complete is None and last_attempt.status == STATUS_IN_PROGRESS:
        return 'WARNING'
    return 'WARNING'


def build_type_summaries(
    session: Session, *, filter_type: str | None = None
) -> list[dict[str, Any]]:
    """Builds consolidated health and milestone summaries per type."""
    stmt = select(ConsistencyReport).order_by(
        ConsistencyReport.started_at.desc()
    )
    if filter_type:
        stmt = stmt.where(ConsistencyReport.type == filter_type)

    rows = session.scalars(stmt).all()
    by_type: dict[str, list[ConsistencyReport]] = {}
    for row in rows:
        by_type.setdefault(row.type, []).append(row)

    ordered_types: list[str]
    if filter_type:
        ordered_types = [filter_type]
    else:
        ordered_types = list(CONSISTENCY_CATALOG.keys())
        for extra_type in by_type:
            if extra_type not in CONSISTENCY_CATALOG:
                ordered_types.append(extra_type)

    cutoff_7d = datetime.now(timezone.utc) - timedelta(days=7)
    summaries: list[dict[str, Any]] = []

    for rtype in ordered_types:
        meta = CONSISTENCY_CATALOG.get(rtype)
        type_rows = by_type.get(rtype, [])
        last_attempt = type_rows[0] if type_rows else None
        last_complete = next(
            (
                r
                for r in type_rows
                if r.status == STATUS_COMPLETED and r.coverage == COVERAGE_FULL
            ),
            None,
        )

        consecutive_failures = 0
        for row in type_rows:
            if row.status == STATUS_FAILED:
                consecutive_failures += 1
            else:
                break

        attempts_7d = sum(
            1 for r in type_rows if ensure_utc(r.started_at) >= cutoff_7d
        )

        summaries.append({
            'type': rtype,
            'label': meta.label if meta else rtype,
            'category': meta.category if meta else 'custom',
            'supports_manual': meta.supports_manual if meta else True,
            'requires_full_sweep': (
                meta.requires_full_sweep if meta else False
            ),
            'health': _compute_health(last_attempt, last_complete),
            'consecutive_failures': consecutive_failures,
            'attempts_last_7d': attempts_7d,
            'last_attempt': last_attempt,
            'last_complete_success': last_complete,
        })

    return summaries


def ensure_utc(dt: datetime) -> datetime:
    """Normalizes naive or offset datetimes to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_export_payload(report: ConsistencyReport) -> dict[str, Any]:
    """Projects a ConsistencyReport to the Portuguese export format."""
    extra_result = dict(report.result or {})
    extra_result.setdefault('tipo', report.type)
    extra_result.setdefault('origem', report.origin)
    extra_result.setdefault('cobertura', report.coverage)
    extra_result.setdefault(
        'referencia_em',
        report.reference_at.isoformat() if report.reference_at else None,
    )
    extra_result.setdefault(
        'etl_run_id',
        str(report.etl_run_id) if report.etl_run_id else None,
    )

    return {
        'id': report.id,
        'pipeline_log_id': report.pipeline_log_id,
        'escopo': report.scope,
        'estado': EXPORT_STATUS_MAP.get(report.status, report.status),
        'iniciado_em': report.started_at,
        'finalizado_em': report.ended_at,
        'antes': report.before or {},
        'depois': report.after,
        'resultado': extra_result,
        'erro': report.error,
    }
