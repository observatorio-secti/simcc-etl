import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from simcc_etl.core.settings import Settings

# Context variables for run_id and routine_name
run_id_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'run_id_ctx', default=None
)
routine_name_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'routine_name_ctx', default=None
)


def _inject_context(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    run_id = run_id_ctx.get()
    routine = routine_name_ctx.get()
    if run_id:
        event_dict['run_id'] = run_id
    if routine:
        event_dict['routine'] = routine
    return event_dict


class DualJsonlFileProcessor:
    """Escreve logs em runs/{run_id}.jsonl e {YYYY-MM-DD}.jsonl."""

    def __init__(self, log_dir: Path | None = None):
        self.log_dir = log_dir or Settings().LOG_DIR
        self.runs_dir = self.log_dir / 'runs'
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def __call__(
        self, logger: Any, method_name: str, event_dict: dict[str, Any]
    ) -> dict[str, Any]:
        line = json.dumps(event_dict, default=str)
        now_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        daily_path = self.log_dir / f'{now_date}.jsonl'

        try:
            with daily_path.open('a', encoding='utf-8') as f:
                f.write(line + '\n')
        except Exception:
            pass

        run_id = event_dict.get('run_id')
        if run_id:
            run_path = self.runs_dir / f'{run_id}.jsonl'
            try:
                with run_path.open('a', encoding='utf-8') as f:
                    f.write(line + '\n')
            except Exception:
                pass

        return event_dict


def configure_logging(log_dir: Path | None = None) -> None:
    settings = Settings()
    effective_log_dir = log_dir or settings.LOG_DIR
    effective_log_dir.mkdir(parents=True, exist_ok=True)
    (effective_log_dir / 'runs').mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        format='%(message)s',
        stream=sys.stdout,
        level=logging.INFO,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_context,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt='iso', utc=True),
            DualJsonlFileProcessor(effective_log_dir),
            structlog.dev.ConsoleRenderer()
            if sys.stdout.isatty()
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str = 'simcc_etl'):
    return structlog.get_logger(name)


logger = get_logger()
