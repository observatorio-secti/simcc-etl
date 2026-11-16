from datetime import datetime, timezone
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from simcc_etl.core.consistency import (
    COVERAGE_FULL,
    COVERAGE_NA,
    COVERAGE_PARTIAL,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_IN_PROGRESS,
    STATUS_PARTIAL,
    build_type_summaries,
    capture_snapshot,
    ensure_utc,
    to_export_payload,
)
from simcc_etl.core.database import ConsistencyReport
from simcc_etl.routers import DbSession, verify_api_key

consistency_router = APIRouter(tags=['Consistency'])

ReportStatus = Literal['IN_PROGRESS', 'COMPLETED', 'PARTIAL', 'FAILED']
ReportCoverage = Literal['FULL', 'PARTIAL', 'NA']
HealthStatus = Literal['OK', 'WARNING', 'CRITICAL', 'PENDING']


class ConsistencyReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    etl_run_id: UUID | None
    pipeline_log_id: str | None
    scope: str
    type: str
    origin: str
    coverage: str
    status: str
    started_at: datetime
    ended_at: datetime | None
    reference_at: datetime | None
    before: dict[str, Any]
    after: dict[str, Any] | None
    result: dict[str, Any] | None
    error: str | None


class ConsistencyTypeSummaryResponse(BaseModel):
    type: str
    label: str
    category: str
    supports_manual: bool
    requires_full_sweep: bool
    health: HealthStatus
    consecutive_failures: int
    attempts_last_7d: int
    last_attempt: ConsistencyReportResponse | None
    last_complete_success: ConsistencyReportResponse | None


class ConsistencyCreateRequest(BaseModel):
    type: str = Field(min_length=1, max_length=100)
    status: ReportStatus = STATUS_COMPLETED
    coverage: ReportCoverage = COVERAGE_FULL
    origin: str = Field(default='manual', min_length=1, max_length=32)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    reference_at: datetime | None = None
    before: dict[str, Any] = Field(default_factory=dict)
    after: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    recorded_by: str | None = None
    capture_db_snapshot: bool = False


class ConsistencyUpdateRequest(BaseModel):
    status: ReportStatus | None = None
    coverage: ReportCoverage | None = None
    ended_at: datetime | None = None
    reference_at: datetime | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    edited_by: str = Field(default='admin', min_length=1, max_length=100)
    edit_note: str | None = None


class RelatorioConsistenciaExport(BaseModel):
    id: str
    pipeline_log_id: str | None
    escopo: Literal['simcc'] = 'simcc'
    estado: str
    iniciado_em: datetime
    finalizado_em: datetime | None
    antes: dict[str, Any]
    depois: dict[str, Any] | None
    resultado: dict[str, Any] | None
    erro: str | None


@consistency_router.get(
    '/consistency',
    response_model=list[ConsistencyTypeSummaryResponse],
    dependencies=[Depends(verify_api_key)],
)
def list_consistency_summary(
    session: DbSession,
    report_type: Annotated[str | None, Query(alias='type')] = None,
) -> list[ConsistencyTypeSummaryResponse]:
    """Returns the consolidated consistency status per type without masking
    recent failures or partial sweeps.
    """
    summaries = build_type_summaries(session, filter_type=report_type)
    return [ConsistencyTypeSummaryResponse(**item) for item in summaries]


class PaginationParams:
    """Shared pagination query parameters."""

    def __init__(
        self,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        self.limit = limit
        self.offset = offset


class HistoryQueryParams:
    """Query parameters for filtering consistency history."""

    def __init__(
        self,
        report_type: Annotated[str | None, Query(alias='type')] = None,
        status_filter: Annotated[
            ReportStatus | None, Query(alias='status')
        ] = None,
        origin: str | None = None,
        coverage: ReportCoverage | None = None,
    ):
        self.report_type = report_type
        self.status_filter = status_filter
        self.origin = origin
        self.coverage = coverage


class ExportQueryParams:
    """Parâmetros de consulta para exportação externa em português."""

    def __init__(
        self,
        apenas_ultimos: bool = False,
        desde: datetime | None = None,
        tipo: str | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ):
        self.apenas_ultimos = apenas_ultimos
        self.desde = desde
        self.tipo = tipo
        self.limit = limit
        self.offset = offset


@consistency_router.get(
    '/consistency/history',
    response_model=list[ConsistencyReportResponse],
    dependencies=[Depends(verify_api_key)],
)
def list_consistency_history(
    session: DbSession,
    filters: Annotated[HistoryQueryParams, Depends()],
    pagination: Annotated[PaginationParams, Depends()],
) -> list[ConsistencyReportResponse]:
    """Returns paginated consistency execution history with filters."""
    stmt = select(ConsistencyReport)
    if filters.report_type:
        stmt = stmt.where(ConsistencyReport.type == filters.report_type)
    if filters.status_filter:
        stmt = stmt.where(ConsistencyReport.status == filters.status_filter)
    if filters.origin:
        stmt = stmt.where(ConsistencyReport.origin == filters.origin)
    if filters.coverage:
        stmt = stmt.where(ConsistencyReport.coverage == filters.coverage)

    stmt = (
        stmt.order_by(ConsistencyReport.started_at.desc())
        .offset(pagination.offset)
        .limit(pagination.limit)
    )
    rows = session.scalars(stmt).all()
    return [ConsistencyReportResponse.model_validate(row) for row in rows]


@consistency_router.get(
    '/consistency/types/{report_type}/history',
    response_model=list[ConsistencyReportResponse],
    dependencies=[Depends(verify_api_key)],
)
def list_type_history(
    report_type: str,
    session: DbSession,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[ConsistencyReportResponse]:
    """Returns paginated consistency history for a specific type."""
    stmt = (
        select(ConsistencyReport)
        .where(ConsistencyReport.type == report_type)
        .order_by(ConsistencyReport.started_at.desc())
        .offset(offset)
        .limit(limit)
    )
    rows = session.scalars(stmt).all()
    return [ConsistencyReportResponse.model_validate(row) for row in rows]


@consistency_router.get(
    '/consistency/reports/{report_id}',
    response_model=ConsistencyReportResponse,
    dependencies=[Depends(verify_api_key)],
)
def get_consistency_report(
    report_id: str, session: DbSession
) -> ConsistencyReportResponse:
    """Returns a single consistency report by ID."""
    report = session.get(ConsistencyReport, report_id)
    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Consistency report not found',
        )
    return ConsistencyReportResponse.model_validate(report)


@consistency_router.post(
    '/consistency',
    status_code=status.HTTP_201_CREATED,
    response_model=ConsistencyReportResponse,
    dependencies=[Depends(verify_api_key)],
)
def create_consistency_report(
    payload: ConsistencyCreateRequest,
    session: DbSession,
) -> ConsistencyReportResponse:
    """Creates a manual or external consistency record without acquiring the
    ETL lock (e.g., manual CAPES scraping dates or reference datasets).
    """
    now = datetime.now(timezone.utc)
    started_at = payload.started_at or now
    ended_at = payload.ended_at
    if ended_at is None and payload.status != STATUS_IN_PROGRESS:
        ended_at = now

    before_data = dict(payload.before)
    after_data = dict(payload.after) if payload.after is not None else None
    if payload.capture_db_snapshot:
        snap = capture_snapshot(session, payload.type)
        if not before_data:
            before_data = snap
        if after_data is None and payload.status != STATUS_IN_PROGRESS:
            after_data = snap

    result_data = dict(payload.result) if payload.result is not None else {}
    if payload.recorded_by:
        result_data['_audit'] = {
            'created_by': payload.recorded_by,
            'created_at': now.isoformat(),
            'edits': [],
        }

    coverage = payload.coverage
    if payload.status == STATUS_FAILED and coverage == COVERAGE_FULL:
        coverage = COVERAGE_NA
    elif payload.status == STATUS_PARTIAL and coverage == COVERAGE_FULL:
        coverage = COVERAGE_PARTIAL

    report = ConsistencyReport(
        id=str(uuid4()),
        scope='simcc',
        type=payload.type,
        origin=payload.origin,
        coverage=coverage,
        status=payload.status,
        started_at=started_at,
        ended_at=ended_at,
        reference_at=payload.reference_at or ended_at or started_at,
        before=before_data,
        after=after_data,
        result=result_data or None,
        error=payload.error,
    )
    session.add(report)
    session.commit()
    session.refresh(report)
    return ConsistencyReportResponse.model_validate(report)


@consistency_router.patch(
    '/consistency/{report_id}',
    response_model=ConsistencyReportResponse,
    dependencies=[Depends(verify_api_key)],
)
def update_consistency_report(
    report_id: str,
    payload: ConsistencyUpdateRequest,
    session: DbSession,
) -> ConsistencyReportResponse:
    """Edits an existing consistency report from the SimCC admin console,
    recording an audit trail inside `result._audit.edits`.
    """
    report = session.get(ConsistencyReport, report_id)
    if not report:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Consistency report not found',
        )

    changed_fields: list[str] = []
    direct_fields = (
        'status',
        'coverage',
        'ended_at',
        'reference_at',
        'before',
        'after',
        'error',
    )
    for field_name in direct_fields:
        value = getattr(payload, field_name)
        if value is not None:
            setattr(report, field_name, value)
            changed_fields.append(field_name)

    merged_result = dict(report.result or {})
    if payload.result is not None:
        for key, val in payload.result.items():
            if key != '_audit':
                merged_result[key] = val
        changed_fields.append('result')

    now = datetime.now(timezone.utc)
    if (
        payload.status in {STATUS_COMPLETED, STATUS_PARTIAL, STATUS_FAILED}
        and report.ended_at is None
    ):
        report.ended_at = now
        changed_fields.append('ended_at')

    audit = dict(merged_result.get('_audit') or {})
    edits = list(audit.get('edits') or [])
    edits.append({
        'edited_by': payload.edited_by,
        'edited_at': now.isoformat(),
        'note': payload.edit_note,
        'changed_fields': changed_fields,
    })
    audit['edits'] = edits
    merged_result['_audit'] = audit
    report.result = merged_result

    session.commit()
    session.refresh(report)
    return ConsistencyReportResponse.model_validate(report)


# NOTA DE INTEGRAÇÃO EXTERNA:
# O endpoint `/consistencia/export` e o schema `RelatorioConsistenciaExport`
# usam intencionalmente nomes em português porque projetos externos parceiros
# consomem essa rota diretamente no contrato `relatorio_consistencia`.
# Todo o restante da base interna do `simcc-etl` permanece em inglês.
@consistency_router.get(
    '/consistencia/export',
    response_model=list[RelatorioConsistenciaExport],
    dependencies=[Depends(verify_api_key)],
)
def export_relatorios_consistencia(
    session: DbSession,
    params: Annotated[ExportQueryParams, Depends()],
) -> list[RelatorioConsistenciaExport]:
    """Exporta registros no formato esperado por sistemas externos."""
    if params.apenas_ultimos:
        summaries = build_type_summaries(session, filter_type=params.tipo)
        selected: dict[str, ConsistencyReport] = {}
        desde_utc = ensure_utc(params.desde) if params.desde else None
        for item in summaries:
            for key in ('last_attempt', 'last_complete_success'):
                row = item[key]
                if row is not None and (
                    desde_utc is None
                    or ensure_utc(row.started_at) >= desde_utc
                ):
                    selected[row.id] = row
        ordered = sorted(
            selected.values(),
            key=lambda r: ensure_utc(r.started_at),
            reverse=True,
        )
        page = ordered[params.offset : params.offset + params.limit]
        return [
            RelatorioConsistenciaExport(**to_export_payload(row))
            for row in page
        ]

    stmt = select(ConsistencyReport)
    if params.tipo:
        stmt = stmt.where(ConsistencyReport.type == params.tipo)
    if params.desde:
        stmt = stmt.where(ConsistencyReport.started_at >= params.desde)

    stmt = (
        stmt.order_by(ConsistencyReport.started_at.desc())
        .offset(params.offset)
        .limit(params.limit)
    )
    rows = session.scalars(stmt).all()
    return [
        RelatorioConsistenciaExport(**to_export_payload(row)) for row in rows
    ]
