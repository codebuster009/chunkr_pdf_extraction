from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import ValidationError


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CHUNKR_",
    )
    API_KEY: str                # CHUNKR_API_KEY=...
    URL: str | None = None      # CHUNKR_URL=... (optional, e.g., self-hosted)
    RAISE_ON_FAILURE: bool = True

try:
    settings = Settings()
except ValidationError as e:
    raise RuntimeError(
        "Missing CHUNKR_API_KEY. Put it in .env or export it in your shell."
    ) from e
