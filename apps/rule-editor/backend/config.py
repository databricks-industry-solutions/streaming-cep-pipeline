from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Default Volume path for rule files (override with APP_VOLUME_PATH env var)
    VOLUME_PATH: str = "/Volumes/cep_demo/network/rules_apps"

    # Local development flag (override with APP_IS_LOCAL env var)
    IS_LOCAL: bool = False

    class Config:
        env_prefix = "APP_"


settings = Settings()
