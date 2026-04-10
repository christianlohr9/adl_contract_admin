"""Application configuration via Pydantic v2 settings."""

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "ADL Contract Admin"
    debug: bool = False
    database_url: str = "postgresql+asyncpg://adl:adl_dev@localhost:5432/adl"

    # MFL API configuration
    mfl_league_id: int = 60206
    mfl_year: int = 2026
    mfl_api_key: str = ""
    mfl_username: str = ""
    mfl_password: str = ""
    mfl_base_url: str = "https://api.myfantasyleague.com"
    mfl_request_delay: float = 1.0
    mfl_backfill_request_delay: float = 6.0

    # CORS configuration
    cors_origins: list[str] = ["http://localhost:5173"]

    # Sync scheduler configuration
    sync_interval_hours: int = 6
    sync_enabled: bool = True
    sync_historical_years: list[int] = [2020, 2021, 2022, 2023, 2024, 2025]

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _parse_cors_origins(cls, v: object) -> object:
        """Accept comma-separated string or JSON array for CORS_ORIGINS."""
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("["):
                return v  # let pydantic parse as JSON
            return [item.strip() for item in s.split(",") if item.strip()]
        return v

    @field_validator("sync_historical_years", mode="before")
    @classmethod
    def _parse_sync_historical_years(cls, v: object) -> object:
        """Accept comma-separated string or JSON array for SYNC_HISTORICAL_YEARS."""
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("["):
                return v
            return [int(item.strip()) for item in s.split(",") if item.strip()]
        return v


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()


settings = get_settings()
