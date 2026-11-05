from pathlib import Path

from pydantic import field_validator
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

    # Apache Hop Configuration
    HOP_IMAGE: str = 'gleidsoncosta/simcc-extrator:latest'
    HOP_WRITE_JDBC_URL: str = 'jdbc:postgresql://write_db:5432/simcc_write?user=postgres&password=postgres'
    HOP_ADMIN_JDBC_URL: str = (
        'jdbc:postgresql://admin_db:5432/db?user=db_user&password=db_password'
    )
    DOCKER_NETWORK: str = 'simcc-network'

    # Paths
    STORAGE_DIR: Path = Path('storage')
    # Caminho absoluto, no HOST do daemon Docker, equivalente a STORAGE_DIR.
    # Necessário quando o ETL roda em container: o bind mount dos XMLs no
    # container do Hop é resolvido pelo daemon do host, não pelo ETL.
    HOST_STORAGE_DIR: Path | None = None
    LOG_DIR: Path = Path('logs')

    # External APIs
    ALTERNATIVE_CNPQ_SERVICE: bool = True
    CNPQ_PROXY_URL: str = 'https://simcc.uesc.br/v3/api'
    OPENALEX_API_KEY: str | None = None
    OPENALEX_BATCH_COMMIT_SIZE: int = 50

    @field_validator('HOST_STORAGE_DIR', mode='before')
    @classmethod
    def _empty_host_storage_as_none(cls, value):
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @property
    def xml_current_path(self) -> Path:
        return self.STORAGE_DIR / 'xml' / 'current'

    @property
    def xml_run_path(self) -> Path:
        return self.STORAGE_DIR / 'xml' / 'run_current'

    @property
    def seed_dir(self) -> Path:
        return self.STORAGE_DIR / 'seed'
