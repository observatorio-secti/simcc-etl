import time
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.database import (
    EtlRun,
    create_etl_tables,
    get_write_engine,
    get_write_session,
)
from simcc_etl.core.logging import (
    get_logger,
    routine_name_ctx,
    run_id_ctx,
)
from simcc_etl.routines import (
    aggregate_researcher_production,
    build_search_dictionary,
    calculate_production_index,
    classify_article_themes,
    classify_researchers,
    enrich_from_openalex,
    export_oda_research_groups,
    extract_lattes_hop,
    import_capes_programs,
    import_program_research_lines,
    publish_to_read_db,
    resolve_cnpq_lattes_id,
    sanitize_lattes_data,
    soap_lattes,
)

logger = get_logger('simcc_etl.pipeline')

ETL_LOCK_ID = 9001001


class AlreadyRunningError(Exception):
    """Lançado quando uma tentativa de adquirir o lock concorrente falha."""


# Catálogo de rotinas mapeadas
ROUTINES: dict[str, Callable[..., Stats]] = {
    'hop_seed': extract_lattes_hop.run_seed,
    'import_capes_programs': import_capes_programs.run_programs,
    'import_capes_ratings': import_capes_programs.run_ratings,
    'soap_lattes': soap_lattes.run,
    'hop_index': extract_lattes_hop.run_index,
    'sanitize_lattes_data': sanitize_lattes_data.run,
    'aggregate_researcher_production': aggregate_researcher_production.run,
    'calculate_researcher_indprod': (
        calculate_production_index.run_researcher_indprod
    ),
    'calculate_graduate_program_indprod': (
        calculate_production_index.run_graduate_program_indprod
    ),
    'calculate_research_group_indprod': (
        calculate_production_index.run_research_group_indprod
    ),
    'calculate_production_index': calculate_production_index.run_all,
    'indprod': calculate_production_index.run_all,
    'classify_researchers': classify_researchers.run,
    'build_search_dictionary': build_search_dictionary.run,
    'resolve_cnpq_lattes_id': resolve_cnpq_lattes_id.run,
    'enrich_from_openalex': enrich_from_openalex.run,
    'import_program_research_lines': import_program_research_lines.run,
    'export_oda_research_groups': export_oda_research_groups.run,
    'classify_article_themes': classify_article_themes.run,
    'publish_to_read_db': publish_to_read_db.run,
    'refresh_search_views': publish_to_read_db.run,
}


def register_routine(name: str, fn: Callable[..., Stats]) -> None:
    ROUTINES[name] = fn


def try_acquire_lock(conn) -> bool:
    """Tenta obter um lock consultivo exclusivo no PostgreSQL."""
    sql = text('SELECT pg_try_advisory_lock(:id)')
    return bool(conn.execute(sql, {'id': ETL_LOCK_ID}).scalar())


def release_lock(conn) -> bool:
    """Libera o lock consultivo exclusivo no PostgreSQL."""
    sql = text('SELECT pg_advisory_unlock(:id)')
    return bool(conn.execute(sql, {'id': ETL_LOCK_ID}).scalar())


def get_available_routines() -> list[str]:
    return list(ROUTINES.keys())


def run_pipeline(  # noqa: C901, PLR0912, PLR0915
    routines: list[str] | None = None,
    lattes_ids: list[str] | None = None,
    triggered_by: str = 'system',
    run_id: UUID | None = None,
) -> EtlRun:
    """Executa a sequência de rotinas com advisory lock e histórico."""
    engine = get_write_engine()
    create_etl_tables(engine)

    effective_run_id = run_id or uuid4()
    run_id_str = str(effective_run_id)
    run_id_ctx.set(run_id_str)

    target_routines = routines or get_available_routines()

    # 1. Abre conexão dedicada para manter o lock durante toda a execução
    lock_conn = engine.connect()
    if not try_acquire_lock(lock_conn):
        lock_conn.close()
        logger.warning(
            'Falha ao adquirir lock: outro ETL já está em execução.',
            run_id=run_id_str,
        )
        raise AlreadyRunningError('Outro ETL já está em execução.')

    session: Session = get_write_session(engine)
    etl_run = EtlRun(
        id=effective_run_id,
        status='RUNNING',
        triggered_by=triggered_by,
        routines=target_routines,
        lattes_ids=lattes_ids,
        steps={},
    )
    session.add(etl_run)
    session.commit()

    logger.info(
        'Iniciando pipeline de ETL',
        run_id=run_id_str,
        routines=target_routines,
        total_routines=len(target_routines),
        lattes_ids_count=len(lattes_ids) if lattes_ids else 0,
    )

    overall_error: Exception | None = None

    try:
        for idx, routine_name in enumerate(target_routines, start=1):
            routine_name_ctx.set(routine_name)
            etl_run.current_routine = routine_name
            session.commit()

            fn = ROUTINES.get(routine_name)
            if not fn:
                err_msg = f'Rotina {routine_name} não encontrada no catálogo.'
                logger.error(err_msg, routine=routine_name)
                etl_run.steps[routine_name] = {
                    'status': 'FAILED',
                    'error': err_msg,
                }
                session.commit()
                overall_error = ValueError(err_msg)
                break

            logger.info(
                f'[{idx}/{len(target_routines)}] Rotina: {routine_name}',
                routine=routine_name,
            )
            step_start = time.perf_counter()

            try:
                # Executa a função da rotina
                stats = fn(session, lattes_ids=lattes_ids)
                duration_ms = int((time.perf_counter() - step_start) * 1000)

                etl_run.steps[routine_name] = {
                    'status': 'SUCCEEDED',
                    'duration_ms': duration_ms,
                    'found': stats.found,
                    'succeeded': stats.succeeded,
                    'failed': stats.failed,
                }
                session.commit()
                logger.info(
                    f'Rotina {routine_name} concluída com sucesso',
                    routine=routine_name,
                    duration_ms=duration_ms,
                    found=stats.found,
                    succeeded=stats.succeeded,
                    failed=stats.failed,
                )
            except Exception as e:
                duration_ms = int((time.perf_counter() - step_start) * 1000)
                logger.error(
                    f'Falha na rotina {routine_name}: {str(e)}',
                    routine=routine_name,
                    duration_ms=duration_ms,
                    error=str(e),
                )
                etl_run.steps[routine_name] = {
                    'status': 'FAILED',
                    'duration_ms': duration_ms,
                    'error': str(e),
                }
                session.commit()
                overall_error = e
                break

        if overall_error:
            etl_run.status = 'FAILED'
            etl_run.error_message = str(overall_error)
        else:
            etl_run.status = 'SUCCEEDED'

        etl_run.current_routine = None
        etl_run.ended_at = datetime.now(timezone.utc)
        session.commit()

    finally:
        session.close()
        try:
            release_lock(lock_conn)
        finally:
            lock_conn.close()

    logger.info(
        f'Pipeline finalizado com status: {etl_run.status}',
        run_id=run_id_str,
        status=etl_run.status,
    )

    if overall_error:
        raise overall_error

    return etl_run
