from pathlib import Path

from simcc_etl.core.settings import Settings


def test_settings_defaults():
    settings = Settings()
    assert '5435' in settings.WRITE_DATABASE_URL
    assert '5434' in settings.READ_DATABASE_URL
    assert '5433' in settings.ADMIN_DATABASE_URL
    assert settings.xml_current_path == Path('storage/xml/current')
    assert settings.xml_run_path == Path('storage/xml/run_current')
