from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "argus"
    postgres_password: str = "argus"
    postgres_db: str = "argus"
    database_url: str | None = None

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "argus"
    minio_secret_key: str = "argussecret"
    minio_bucket: str = "argus-html"
    minio_secure: bool = False

    # OTel
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    otel_service_name: str = "argus"

    # Crawler
    crawler_concurrency: int = 10
    crawler_timeout_seconds: int = 30
    crawler_user_agent: str = "ArgusBot/0.1 (+https://github.com/argus)"
    crawler_proxy_urls: str = ""

    # Rate limiting
    rate_limit_tokens_per_second: float = 1.0
    rate_limit_burst: int = 5

    # Retry
    retry_max_attempts: int = 5
    retry_base_delay_seconds: float = 1.0

    # Incremental crawl
    incremental_recrawl_ttl_seconds: int = 3600
    dlq_max_replays: int = 3

    # Search
    search_similarity_threshold: float = 0.1
    search_default_limit: int = 20
    search_max_limit: int = 100

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def async_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
