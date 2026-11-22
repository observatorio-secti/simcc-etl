import inspect
import time
from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
from typing import TypedDict
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from simcc_etl.common import Stats
from simcc_etl.core.consistency import (
    RoutineExecutionMeta,
    RoutineOutcome,
    finish_consistency_reports,
    start_consistency_reports,
)
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
    enrich_researcher_institution,
    export_oda_research_groups,
    extract_lattes_hop,
    import_capes_programs,
    import_program_research_lines,
    publish_to_read_db,
    remove_researchers,
    resolve_cnpq_lattes_id,
    sanitize_lattes_data,
    seed_geo_reference,
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
    'import_capes_researchers': import_capes_programs.run_researchers,
    'soap_lattes': soap_lattes.run,
    'hop_index': extract_lattes_hop.run_index,
    'sanitize_lattes_data': sanitize_lattes_data.run,
    'remove_researchers': remove_researchers.run,
    'seed_geo_reference': seed_geo_reference.run,
    'enrich_researcher_institution': enrich_researcher_institution.run,
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


DEFAULT_ROUTINES: list[str] = [
    'soap_lattes',
    'hop_index',
    'import_capes_programs',
    'import_capes_ratings',
    'import_capes_researchers',
    'sanitize_lattes_data',
    'remove_researchers',
    'seed_geo_reference',
    'enrich_researcher_institution',
    'aggregate_researcher_production',
    'export_oda_research_groups',
    'calculate_production_index',
    'classify_researchers',
    'build_search_dictionary',
    'resolve_cnpq_lattes_id',
    'publish_to_read_db',
]

# Rotinas cuja falha é registrada no step, mas não interrompe o pipeline.
NON_BLOCKING_ROUTINES: frozenset[str] = frozenset({
    'export_oda_research_groups',
})


def get_default_routines() -> list[str]:
    """Retorna a sequência padrão do pipeline diário (exclui setup de seed)."""
    return list(DEFAULT_ROUTINES)


def try_acquire_lock(conn) -> bool:
    """Tenta obter um lock consultivo exclusivo no PostgreSQL."""
    sql = text('SELECT pg_try_advisory_lock(:id)')
    return bool(conn.execute(sql, {'id': ETL_LOCK_ID}).scalar())


def release_lock(conn) -> bool:
    """Libera o lock consultivo exclusivo no PostgreSQL."""
    sql = text('SELECT pg_advisory_unlock(:id)')
    return bool(conn.execute(sql, {'id': ETL_LOCK_ID}).scalar())


class RoutineCategory(str, Enum):
    """Categorias fechadas de rotinas para exibição no console."""

    COLLECTION = 'collection'
    PROCESSING = 'processing'
    CONSOLIDATION = 'consolidation'
    INDICATORS = 'indicators'
    INTEGRATION = 'integration'


class RoutineMetadata(TypedDict):
    id: str
    title: str
    description: str
    category: RoutineCategory


ROUTINE_ALIASES: frozenset[str] = frozenset({
    'indprod',
    'refresh_search_views',
})

ROUTINE_METADATA: dict[str, RoutineMetadata] = {
    'hop_seed': {
        'id': 'hop_seed',
        'title': 'Carga inicial Hop (Lattes)',
        'description': (
            'Executa a extração inicial completa de currículos Lattes via Hop.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'import_capes_programs': {
        'id': 'import_capes_programs',
        'title': 'Importar programas CAPES',
        'description': (
            'Importa e atualiza os programas de pós-graduação a partir dos '
            'dados da CAPES.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'import_capes_ratings': {
        'id': 'import_capes_ratings',
        'title': 'Importar conceitos CAPES',
        'description': (
            'Importa as notas e avaliações dos programas de pós-graduação '
            'da CAPES.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'import_capes_researchers': {
        'id': 'import_capes_researchers',
        'title': 'Importar docentes e discentes CAPES',
        'description': (
            'Importa vínculos de pesquisadores e discentes dos programas CAPES.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'soap_lattes': {
        'id': 'soap_lattes',
        'title': 'Coletar currículos SOAP Lattes',
        'description': (
            'Verifica atualizações e baixa os XMLs de currículos Lattes via '
            'serviço SOAP do CNPq.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'hop_index': {
        'id': 'hop_index',
        'title': 'Indexar currículos Hop',
        'description': (
            'Processa e indexa no banco de escrita os currículos extraídos '
            'pelo pipeline Hop.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'sanitize_lattes_data': {
        'id': 'sanitize_lattes_data',
        'title': 'Higienizar dados Lattes',
        'description': (
            'Normaliza textos, datas, identificadores e registros brutos '
            'importados do Lattes.'
        ),
        'category': RoutineCategory.PROCESSING,
    },
    'remove_researchers': {
        'id': 'remove_researchers',
        'title': 'Remover pesquisadores',
        'description': (
            'Remove pesquisadores marcados para exclusão e limpa seus '
            'vínculos relacionados.'
        ),
        'category': RoutineCategory.PROCESSING,
    },
    'enrich_researcher_institution': {
        'id': 'enrich_researcher_institution',
        'title': 'Vincular instituições',
        'description': (
            'Associa pesquisadores às suas instituições e unidades de '
            'referência.'
        ),
        'category': RoutineCategory.CONSOLIDATION,
    },
    'aggregate_researcher_production': {
        'id': 'aggregate_researcher_production',
        'title': 'Agregar produção científica',
        'description': (
            'Consolida totais e séries históricas de produção por pesquisador.'
        ),
        'category': RoutineCategory.CONSOLIDATION,
    },
    'calculate_researcher_indprod': {
        'id': 'calculate_researcher_indprod',
        'title': 'Índice de produção (pesquisadores)',
        'description': (
            'Calcula o indicador de produção científica individual dos '
            'pesquisadores.'
        ),
        'category': RoutineCategory.INDICATORS,
    },
    'calculate_graduate_program_indprod': {
        'id': 'calculate_graduate_program_indprod',
        'title': 'Índice de produção (programas)',
        'description': (
            'Calcula o indicador de produção dos programas de pós-graduação.'
        ),
        'category': RoutineCategory.INDICATORS,
    },
    'calculate_research_group_indprod': {
        'id': 'calculate_research_group_indprod',
        'title': 'Índice de produção (grupos)',
        'description': (
            'Calcula o indicador de produção dos grupos de pesquisa.'
        ),
        'category': RoutineCategory.INDICATORS,
    },
    'calculate_production_index': {
        'id': 'calculate_production_index',
        'title': 'Calcular índices de produção',
        'description': (
            'Executa o cálculo completo dos índices de produção para '
            'pesquisadores, programas e grupos.'
        ),
        'category': RoutineCategory.INDICATORS,
    },
    'classify_researchers': {
        'id': 'classify_researchers',
        'title': 'Classificar pesquisadores',
        'description': (
            'Classifica pesquisadores por áreas de atuação, estratos e '
            'critérios analíticos.'
        ),
        'category': RoutineCategory.INDICATORS,
    },
    'build_search_dictionary': {
        'id': 'build_search_dictionary',
        'title': 'Construir dicionário de busca',
        'description': (
            'Gera e atualiza os termos e tokens usados na busca textual.'
        ),
        'category': RoutineCategory.PROCESSING,
    },
    'resolve_cnpq_lattes_id': {
        'id': 'resolve_cnpq_lattes_id',
        'title': 'Resolver IDs Lattes (CNPq)',
        'description': (
            'Consulta o CNPq para resolver identificadores Lattes ausentes ou '
            'desatualizados.'
        ),
        'category': RoutineCategory.INTEGRATION,
    },
    'enrich_from_openalex': {
        'id': 'enrich_from_openalex',
        'title': 'Enriquecer com OpenAlex',
        'description': (
            'Complementa metadados de artigos e citações usando a API do '
            'OpenAlex.'
        ),
        'category': RoutineCategory.INTEGRATION,
    },
    'import_program_research_lines': {
        'id': 'import_program_research_lines',
        'title': 'Importar linhas de pesquisa',
        'description': (
            'Importa e associa as linhas de pesquisa dos programas de '
            'pós-graduação.'
        ),
        'category': RoutineCategory.COLLECTION,
    },
    'export_oda_research_groups': {
        'id': 'export_oda_research_groups',
        'title': 'Exportar grupos para o ODA',
        'description': (
            'Exporta os dados consolidados de grupos de pesquisa para o '
            'ecossistema ODA.'
        ),
        'category': RoutineCategory.INTEGRATION,
    },
    'classify_article_themes': {
        'id': 'classify_article_themes',
        'title': 'Classificar temas de artigos',
        'description': (
            'Classifica artigos científicos em temas e áreas de conhecimento.'
        ),
        'category': RoutineCategory.PROCESSING,
    },
    'publish_to_read_db': {
        'id': 'publish_to_read_db',
        'title': 'Publicar no banco de leitura',
        'description': (
            'Publica os dados consolidados no banco de leitura, atualiza as '
            'visões materializadas e invalida o cache de busca.'
        ),
        'category': RoutineCategory.CONSOLIDATION,
    },
}


def get_available_routines() -> list[str]:
    return list(ROUTINES.keys())


def get_routine_catalog() -> list[RoutineMetadata]:
    """Retorna os metadados das rotinas canônicas (sem aliases)."""
    return [
        ROUTINE_METADATA[name]
        for name in ROUTINES
        if name not in ROUTINE_ALIASES
    ]


def run_pipeline(  # noqa: C901, PLR0912, PLR0915
    routines: list[str] | None = None,
    lattes_ids: list[str] | None = None,
    triggered_by: str = 'system',
    run_id: UUID | None = None,
    force: bool = False,
) -> EtlRun:
    """Executa a sequência de rotinas com advisory lock e histórico."""
    engine = get_write_engine()
    create_etl_tables(engine)

    effective_run_id = run_id or uuid4()
    run_id_str = str(effective_run_id)
    run_id_ctx.set(run_id_str)

    target_routines = routines or get_default_routines()

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
                etl_run.steps = {
                    **(etl_run.steps or {}),
                    routine_name: {
                        'status': 'FAILED',
                        'error': err_msg,
                    },
                }
                session.commit()
                overall_error = ValueError(err_msg)
                break

            logger.info(
                f'[{idx}/{len(target_routines)}] Rotina: {routine_name}',
                routine=routine_name,
            )
            step_start = time.perf_counter()
            consistency_meta = RoutineExecutionMeta(
                routine_name=routine_name,
                etl_run_id=effective_run_id,
                origin=triggered_by,
                lattes_ids=lattes_ids,
            )
            consistency_ids = start_consistency_reports(
                engine, consistency_meta
            )

            try:
                # Executa a função da rotina
                routine_kwargs: dict[str, object] = {
                    'lattes_ids': lattes_ids,
                }
                if force and 'force' in inspect.signature(fn).parameters:
                    routine_kwargs['force'] = True
                stats = fn(session, **routine_kwargs)
                duration_ms = int((time.perf_counter() - step_start) * 1000)

                etl_run.steps = {
                    **(etl_run.steps or {}),
                    routine_name: {
                        'status': 'SUCCEEDED',
                        'duration_ms': duration_ms,
                        'found': stats.found,
                        'succeeded': stats.succeeded,
                        'failed': stats.failed,
                    },
                }
                session.commit()
            except Exception as e:
                duration_ms = int((time.perf_counter() - step_start) * 1000)
                is_non_blocking = routine_name in NON_BLOCKING_ROUTINES
                session.rollback()
                step_payload: dict[str, object] = {
                    'status': 'FAILED',
                    'duration_ms': duration_ms,
                    'error': str(e),
                }
                if is_non_blocking:
                    step_payload['non_blocking'] = True
                etl_run.steps = {
                    **(etl_run.steps or {}),
                    routine_name: step_payload,
                }
                session.commit()
                finish_consistency_reports(
                    engine,
                    consistency_ids,
                    consistency_meta,
                    RoutineOutcome(duration_ms=duration_ms, error=str(e)),
                )

                if is_non_blocking:
                    logger.warning(
                        f'Falha na rotina não bloqueante {routine_name}: '
                        f'{str(e)}. Continuando pipeline.',
                        routine=routine_name,
                        duration_ms=duration_ms,
                        error=str(e),
                        non_blocking=True,
                    )
                    continue

                logger.error(
                    f'Falha na rotina {routine_name}: {str(e)}',
                    routine=routine_name,
                    duration_ms=duration_ms,
                    error=str(e),
                )
                overall_error = e
                break
            else:
                finish_consistency_reports(
                    engine,
                    consistency_ids,
                    consistency_meta,
                    RoutineOutcome(stats=stats, duration_ms=duration_ms),
                )
                logger.info(
                    f'Rotina {routine_name} concluída com sucesso',
                    routine=routine_name,
                    duration_ms=duration_ms,
                    found=stats.found,
                    succeeded=stats.succeeded,
                    failed=stats.failed,
                )

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
