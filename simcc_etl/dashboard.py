import json
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy import select

from simcc_etl.core.database import EtlRun, get_write_session
from simcc_etl.core.settings import Settings

dashboard_router = APIRouter(prefix='/dashboard', tags=['Dashboard'])

TEMPLATE_PATH = (
    Path(__file__).resolve().parent / 'templates' / 'dashboard.html'
)


def _get_log_dir() -> Path:
    """Resolve o diretório de logs garantindo compatibilidade de cwd."""
    cfg_dir = Settings().LOG_DIR
    if cfg_dir.is_absolute() and cfg_dir.exists():
        return cfg_dir
    pkg_dir = Path(__file__).resolve().parents[1] / cfg_dir
    if pkg_dir.exists():
        return pkg_dir
    return Path.cwd() / cfg_dir


def get_dashboard_html() -> str:
    if TEMPLATE_PATH.exists():
        return TEMPLATE_PATH.read_text(encoding='utf-8')
    return (
        '<!DOCTYPE html><html><body><h1>Dashboard template not found</h1>'
        '</body></html>'
    )


def _parse_run_from_file(file_path: Path) -> dict[str, Any]:
    """Extrai metadados essenciais de um arquivo {run_id}.jsonl."""
    run_id = file_path.stem
    stat = file_path.stat()
    file_size = stat.st_size
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

    first_lines: list[dict[str, Any]] = []
    try:
        with file_path.open('r', encoding='utf-8', errors='replace') as f:
            for _ in range(10):
                line = f.readline()
                if not line:
                    break
                s = line.strip()
                if s:
                    try:
                        first_lines.append(json.loads(s))
                    except Exception:
                        pass
    except Exception:
        pass

    last_lines: list[dict[str, Any]] = []
    try:
        with file_path.open('rb') as f:
            f.seek(0, 2)
            size = f.tell()
            buffer_size = min(size, 16384)
            f.seek(size - buffer_size)
            raw = f.read().decode('utf-8', errors='replace')
            for line in raw.strip().splitlines()[-25:]:
                s = line.strip()
                if s:
                    try:
                        last_lines.append(json.loads(s))
                    except Exception:
                        pass
    except Exception:
        pass

    first = first_lines[0] if first_lines else {}
    routines = first.get('routines', [])
    started_at = first.get('timestamp')
    triggered_by = first.get('triggered_by', 'system')

    status_str = 'RUNNING'
    current_routine = None
    error_message = None
    ended_at = None

    for entry in reversed(last_lines):
        event = entry.get('event', '')
        if 'Pipeline finalizado com status:' in event:
            status_str = entry.get('status', 'SUCCEEDED')
            ended_at = entry.get('timestamp')
            break
        if entry.get('level') == 'error' and not error_message:
            error_message = entry.get('error') or entry.get('message')

    if status_str == 'RUNNING':
        # Se modificado há mais de 24h sem encerramento, marca como UNKNOWN
        diff = (datetime.now(timezone.utc) - mtime).total_seconds()
        if diff > 86400:
            status_str = 'UNKNOWN'
        for entry in reversed(last_lines):
            if entry.get('routine'):
                current_routine = entry.get('routine')
                break

    steps: dict[str, Any] = {}
    for entry in last_lines:
        ev = entry.get('event', '')
        rt = entry.get('routine')
        if rt and 'concluída com sucesso' in ev:
            steps[rt] = {
                'status': 'SUCCEEDED',
                'duration_ms': entry.get('duration_ms'),
                'found': entry.get('found'),
                'succeeded': entry.get('succeeded'),
                'failed': entry.get('failed'),
            }
        elif rt and 'Falha na rotina' in ev:
            steps[rt] = {
                'status': 'FAILED',
                'duration_ms': entry.get('duration_ms'),
                'error': entry.get('error'),
            }

    return {
        'id': run_id,
        'status': status_str,
        'started_at': started_at,
        'ended_at': ended_at,
        'triggered_by': triggered_by,
        'current_routine': current_routine,
        'routines': routines,
        'steps': steps,
        'error_message': error_message,
        'file_size': file_size,
        'mtime': mtime.isoformat(),
    }


def _get_runs_from_filesystem(limit: int = 50) -> list[dict[str, Any]]:
    runs_dir = _get_log_dir() / 'runs'
    if not runs_dir.exists():
        return []

    files = sorted(
        runs_dir.glob('*.jsonl'),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    results = []
    for f in files[:limit]:
        try:
            results.append(_parse_run_from_file(f))
        except Exception:
            pass
    return results


def _get_runs_from_db(limit: int = 50) -> list[dict[str, Any]] | None:
    try:
        session = get_write_session()
        try:
            stmt = (
                select(EtlRun).order_by(EtlRun.started_at.desc()).limit(limit)
            )
            runs = session.scalars(stmt).all()
            return [
                {
                    'id': str(r.id),
                    'status': r.status,
                    'started_at': (
                        r.started_at.isoformat() if r.started_at else None
                    ),
                    'ended_at': (
                        r.ended_at.isoformat() if r.ended_at else None
                    ),
                    'triggered_by': r.triggered_by,
                    'current_routine': r.current_routine,
                    'routines': r.routines or [],
                    'steps': r.steps or {},
                    'error_message': r.error_message,
                    'file_size': 0,
                }
                for r in runs
            ]
        finally:
            session.close()
    except Exception:
        return None


@dashboard_router.get('', response_class=HTMLResponse)
@dashboard_router.get('/', response_class=HTMLResponse)
def dashboard_view() -> HTMLResponse:
    """Retorna a interface visual do monitor de execuções e logs."""
    return HTMLResponse(content=get_dashboard_html())


@dashboard_router.get('/api/runs')
def list_dashboard_runs(
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> list[dict[str, Any]]:
    """Lista as execuções recentes com fallback automático para arquivos."""
    db_runs = _get_runs_from_db(limit=limit)
    fs_runs = _get_runs_from_filesystem(limit=limit)

    if db_runs is None:
        return fs_runs

    # Mescla execuções do banco com as do sistema de arquivos
    seen = set()
    combined: list[dict[str, Any]] = []
    for r in db_runs:
        seen.add(r['id'])
        combined.append(r)

    for r in fs_runs:
        if r['id'] not in seen:
            combined.append(r)

    # Ordena decrescente por started_at
    combined.sort(
        key=lambda x: x.get('started_at') or x.get('mtime') or '',
        reverse=True,
    )
    return combined[:limit]


@dashboard_router.get('/api/runs/{run_id}')
def get_dashboard_run(run_id: str) -> dict[str, Any]:
    """Retorna detalhes de uma execução específica."""
    try:
        session = get_write_session()
        try:
            uuid_obj = UUID(run_id)
            r = session.scalar(select(EtlRun).where(EtlRun.id == uuid_obj))
            if r:
                return {
                    'id': str(r.id),
                    'status': r.status,
                    'started_at': (
                        r.started_at.isoformat() if r.started_at else None
                    ),
                    'ended_at': (
                        r.ended_at.isoformat() if r.ended_at else None
                    ),
                    'triggered_by': r.triggered_by,
                    'current_routine': r.current_routine,
                    'routines': r.routines or [],
                    'steps': r.steps or {},
                    'error_message': r.error_message,
                }
        finally:
            session.close()
    except Exception:
        pass

    # Fallback para o arquivo em disco
    file_path = _get_log_dir() / 'runs' / f'{run_id}.jsonl'
    if file_path.exists():
        return _parse_run_from_file(file_path)

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail='Run não encontrada',
    )


@dashboard_router.get('/api/runs/{run_id}/logs')
def get_dashboard_run_logs(
    run_id: str,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
) -> dict[str, Any]:
    """Retorna linhas de log de uma execução específica com paginação."""
    run_status = 'UNKNOWN'
    try:
        session = get_write_session()
        try:
            r = session.scalar(select(EtlRun).where(EtlRun.id == UUID(run_id)))
            if r:
                run_status = r.status
        finally:
            session.close()
    except Exception:
        pass

    log_path = _get_log_dir() / 'runs' / f'{run_id}.jsonl'
    if not log_path.exists():
        return {'lines': [], 'next_offset': offset, 'status': run_status}

    lines: list[dict[str, Any]] = []
    with log_path.open('r', encoding='utf-8', errors='replace') as f:
        for raw_line in islice(f, offset, offset + limit):
            s = raw_line.strip()
            if s:
                try:
                    lines.append(json.loads(s))
                except json.JSONDecodeError:
                    pass

    return {
        'lines': lines,
        'next_offset': offset + len(lines),
        'status': run_status,
    }


@dashboard_router.get('/api/runs/{run_id}/download')
def download_run_log(run_id: str) -> FileResponse:
    """Baixa o arquivo .jsonl completo da execução."""
    log_path = _get_log_dir() / 'runs' / f'{run_id}.jsonl'
    if not log_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Arquivo de log não encontrado',
        )
    return FileResponse(
        path=log_path,
        media_type='application/x-ndjson',
        filename=f'{run_id}.jsonl',
    )


@dashboard_router.get('/api/daily')
def list_daily_logs() -> list[dict[str, Any]]:
    """Lista todos os arquivos de log diários disponíveis."""
    log_dir = _get_log_dir()
    if not log_dir.exists():
        return []

    files = sorted(
        [f for f in log_dir.glob('*.jsonl') if f.is_file()],
        key=lambda p: p.name,
        reverse=True,
    )

    results = []
    for f in files:
        stat = f.stat()
        date_str = f.stem
        results.append({
            'date': date_str,
            'filename': f.name,
            'size_bytes': stat.st_size,
            'modified_at': datetime.fromtimestamp(
                stat.st_mtime, tz=timezone.utc
            ).isoformat(),
        })
    return results


@dashboard_router.get('/api/daily/{date_str}/logs')
def get_daily_log_lines(
    date_str: str,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=2000)] = 500,
) -> dict[str, Any]:
    """Retorna linhas de log de uma data específica com paginação."""
    log_path = _get_log_dir() / f'{date_str}.jsonl'
    if not log_path.exists():
        return {'lines': [], 'next_offset': offset}

    lines: list[dict[str, Any]] = []
    with log_path.open('r', encoding='utf-8', errors='replace') as f:
        for raw_line in islice(f, offset, offset + limit):
            s = raw_line.strip()
            if s:
                try:
                    lines.append(json.loads(s))
                except json.JSONDecodeError:
                    pass

    return {
        'lines': lines,
        'next_offset': offset + len(lines),
    }


@dashboard_router.get('/api/daily/{date_str}/download')
def download_daily_log(date_str: str) -> FileResponse:
    """Baixa o arquivo .jsonl diário completo."""
    log_path = _get_log_dir() / f'{date_str}.jsonl'
    if not log_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Arquivo de log diário não encontrado',
        )
    return FileResponse(
        path=log_path,
        media_type='application/x-ndjson',
        filename=f'{date_str}.jsonl',
    )
