"""Application configuration via Pydantic v2 settings."""

from functools import lru_cache

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


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()


settings = get_settings()
