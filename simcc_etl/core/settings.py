from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    # Database URLs (psycopg v3)
    WRITE_DATABASE_URL: str = (
        'postgresql+psycopg://postgres:postgres@localhost:5432/simcc_write'
    )
    READ_DATABASE_URL: str = (
        'postgresql+psycopg://postgres:postgres@localhost:5432/simcc'
    )
    ADMIN_DATABASE_URL: str = (
        'postgresql+psycopg://db_user:db_password@localhost:5432/db'
    )

    # Redis URL
    REDIS_URL: str = 'redis://localhost:6379/0'

    # Security
    ETL_API_KEY: str = 'simcc-etl-secret-key'
    ADMIN_SERVICE_TOKEN: str = ''
    ADMIN_API_URL: str = 'http://localhost:8009'

    # Concurrency / Performance
    SOAP_MAX_WORKERS: int = 16

    # Apache Hop Server (container long-lived; ver serviço `hop` no compose)
    HOP_SERVER_URL: str = 'http://localhost:8080'
    HOP_SERVER_USER: str = 'cluster'
    HOP_SERVER_PASSWORD: str = 'cluster'
    # Tempo máximo (s) de uma execução síncrona de workflow (Index.hwf é
    # longo: o padrão cobre algumas horas).
    HOP_SERVER_TIMEOUT: float = 6 * 60 * 60
    HOP_WRITE_JDBC_URL: str = 'jdbc:postgresql://write_db:5432/simcc_write?user=postgres&password=postgres'
    HOP_ADMIN_JDBC_URL: str = (
        'jdbc:postgresql://admin_db:5432/db?user=db_user&password=db_password'
    )

    # Paths
    STORAGE_DIR: Path = Path('storage')
    LOG_DIR: Path = Path('logs')

    # External APIs
    ALTERNATIVE_CNPQ_SERVICE: bool = True
    CNPQ_PROXY_URL: str = 'https://simcc.uesc.br/v3/api'
    OPENALEX_API_KEY: str | None = None
    OPENALEX_BATCH_COMMIT_SIZE: int = 50

    @property
    def xml_current_path(self) -> Path:
        return self.STORAGE_DIR / 'xml' / 'current'

    @property
    def xml_run_path(self) -> Path:
        return self.STORAGE_DIR / 'xml' / 'run_current'

    @property
    def xml_hop_input_path(self) -> Path:
        """Pasta fixa lida pelo Hop Server (montada em datasets/lattes_xml)."""
        return self.STORAGE_DIR / 'xml' / 'hop_input'

    @property
    def seed_dir(self) -> Path:
        return self.STORAGE_DIR / 'seed'
