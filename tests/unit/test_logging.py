import json
from uuid import uuid4

from simcc_etl.core.logging import (
    configure_logging,
    get_logger,
    routine_name_ctx,
    run_id_ctx,
)


def test_structured_logging(tmp_path):
    configure_logging(tmp_path)
    logger = get_logger('test_logger')

    run_id = str(uuid4())
    run_id_ctx.set(run_id)
    routine_name_ctx.set('test_routine')

    logger.info('Test message', items=10)

    # Verifica arquivo de run
    run_file = tmp_path / 'runs' / f'{run_id}.jsonl'
    assert run_file.exists()

    with run_file.open('r', encoding='utf-8') as f:
        line = f.readline()
        data = json.loads(line)
        assert data['event'] == 'Test message'
        assert data['run_id'] == run_id
        assert data['routine'] == 'test_routine'
        assert data['items'] == 10
