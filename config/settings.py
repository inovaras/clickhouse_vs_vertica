from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent

class ClickhouseSettings(BaseModel):
    host: str = "clickhouse-node1"
    port: int = 8123
    database: str = "example"
    user: str = "default"
    password: str = ""
    table: str = "events"

class VerticaSettings(BaseModel):
    host: str = "vertica"
    port: int = 5433
    database: str = "docker"
    user: str = "dbadmin"
    password: str = ""
    vertica_schema: str = "public"
    table: str = "events"


class Settings(BaseSettings):
    batch_size: int = 1000
    run_interval_seconds: int = 5
    run_once: bool = False
    log_level: str = "info"

    clickhouse: ClickhouseSettings
    vertica: VerticaSettings

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

settings = Settings()