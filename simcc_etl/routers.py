import json
from collections.abc import Generator
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    Header,
    HTTPException,
    Query,
    status,
)
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from simcc_etl.core.database import EtlRun, get_write_session
from simcc_etl.core.settings import Settings
from simcc_etl.pipeline import (
    AlreadyRunningError,
    get_available_routines,
    run_pipeline,
)

router = APIRouter(tags=['ETL'])


def get_db() -> Generator[Session, None, None]:
    session = get_write_session()
    try:
        yield session
    finally:
        session.close()


DbSession = Annotated[Session, Depends(get_db)]


def verify_api_key(
    x_etl_api_key: Annotated[str | None, Header()] = None,
) -> None:
    settings = Settings()
    if not x_etl_api_key or x_etl_api_key != settings.ETL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid or missing X-ETL-API-Key',
        )


class RunTriggerRequest(BaseModel):
    routines: list[str] | None = None
    lattes_ids: list[str] | None = None
    triggered_by: str = 'admin'


class RunTriggerResponse(BaseModel):
    run_id: UUID
    status: str


class RunDetailResponse(BaseModel):
    id: UUID
    status: str
    started_at: datetime
    ended_at: datetime | None
    triggered_by: str
    current_routine: str | None
    routines: list[str]
    lattes_ids: list[str] | None
    steps: dict[str, Any]
    error_message: str | None


class RunLogsResponse(BaseModel):
    lines: list[dict[str, Any]]
    next_offset: int
    status: str


@router.get(
    '/routines',
    response_model=list[str],
    dependencies=[Depends(verify_api_key)],
)
def list_routines() -> list[str]:
    """Retorna todas as rotinas disponíveis no catálogo."""
    return get_available_routines()


@router.post(
    '/runs',
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunTriggerResponse,
    dependencies=[Depends(verify_api_key)],
)
def trigger_run(
    payload: RunTriggerRequest,
    background_tasks: BackgroundTasks,
) -> RunTriggerResponse:
    """Dispara uma execução de ETL em segundo plano."""
    run_id = uuid4()

    def _execute():
        try:
            run_pipeline(
                routines=payload.routines,
                lattes_ids=payload.lattes_ids,
                triggered_by=payload.triggered_by,
                run_id=run_id,
            )
        except AlreadyRunningError:
            pass
        except Exception:
            pass

    background_tasks.add_task(_execute)
    return RunTriggerResponse(run_id=run_id, status='ACCEPTED')


@router.get(
    '/runs',
    response_model=list[RunDetailResponse],
    dependencies=[Depends(verify_api_key)],
)
def list_runs(
    session: DbSession,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> list[RunDetailResponse]:
    """Lista o histórico recente de execuções."""
    stmt = select(EtlRun).order_by(EtlRun.started_at.desc()).limit(limit)
    runs = session.scalars(stmt).all()
    return [
        RunDetailResponse(
            id=r.id,
            status=r.status,
            started_at=r.started_at,
            ended_at=r.ended_at,
            triggered_by=r.triggered_by,
            current_routine=r.current_routine,
            routines=r.routines or [],
            lattes_ids=r.lattes_ids,
            steps=r.steps or {},
            error_message=r.error_message,
        )
        for r in runs
    ]


@router.get(
    '/runs/{run_id}',
    response_model=RunDetailResponse,
    dependencies=[Depends(verify_api_key)],
)
def get_run(run_id: UUID, session: DbSession) -> RunDetailResponse:
    """Retorna os detalhes e o progresso de uma tentativa."""
    r = session.scalar(select(EtlRun).where(EtlRun.id == run_id))
    if not r:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Run not found',
        )
    return RunDetailResponse(
        id=r.id,
        status=r.status,
        started_at=r.started_at,
        ended_at=r.ended_at,
        triggered_by=r.triggered_by,
        current_routine=r.current_routine,
        routines=r.routines or [],
        lattes_ids=r.lattes_ids,
        steps=r.steps or {},
        error_message=r.error_message,
    )


@router.get(
    '/runs/{run_id}/logs',
    response_model=RunLogsResponse,
    dependencies=[Depends(verify_api_key)],
)
def get_run_logs(
    run_id: UUID,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
) -> RunLogsResponse:
    """Retorna os logs da tentativa a partir de uma linha offset."""
    run_status = 'UNKNOWN'
    try:
        session = get_write_session()
        try:
            r = session.scalar(select(EtlRun).where(EtlRun.id == run_id))
            if r:
                run_status = r.status
        finally:
            session.close()
    except Exception:
        pass

    log_path = Path(Settings().LOG_DIR) / 'runs' / f'{run_id}.jsonl'
    if not log_path.exists():
        return RunLogsResponse(lines=[], next_offset=offset, status=run_status)

    lines: list[dict[str, Any]] = []
    with log_path.open('r', encoding='utf-8') as f:
        for raw_line in islice(f, offset, offset + limit):
            s = raw_line.strip()
            if s:
                try:
                    lines.append(json.loads(s))
                except json.JSONDecodeError:
                    pass

    return RunLogsResponse(
        lines=lines,
        next_offset=offset + len(lines),
        status=run_status,
    )
