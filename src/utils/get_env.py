from __future__ import annotations
from typing import Any
from functools import lru_cache
import os
from pathlib import Path
from pydantic import Field, BaseModel
from pydantic_settings import (
    BaseSettings, SettingsConfigDict,
    DotEnvSettingsSource, PydanticBaseSettingsSource)
from utils import rootpath


_ENVPATH = Path(rootpath) / ".env"
_SECPATH = Path(rootpath) / ".secrets"

class KV(BaseModel):
    model_config = SettingsConfigDict(extra="allow")  # accept any keys

class Environment(BaseSettings):
    model_config = SettingsConfigDict(
        env_file = str(_ENVPATH),
        env_file_encoding="utf-8",
        extra="allow",
        case_sensitive=False,
    )

    init: dict[str, Any]   = Field(default_factory=dict)
    os: dict[str, Any]    = Field(default_factory=dict)
    dotenv: KV = Field(default_factory=KV)
    secrets: KV = Field(default_factory=KV)


    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource | None,
        file_secret_settings: PydanticBaseSettingsSource | None,
    ):

        # compute raw once
        dotenv_raw  = DotEnvSettingsSource(settings_cls, env_file=str(_ENVPATH), env_file_encoding="utf-8")()
        secrets_raw = DotEnvSettingsSource(settings_cls, env_file=str(_SECPATH), env_file_encoding="utf-8")()
        os_raw  = dict(os.environ)
        init_raw    = init_settings() # kwargs passed to Settings(...)
        custom_fields = {"rootpath": str(rootpath), "project":Path(rootpath).name}

        def aggregate():
            return {
                "init": init_raw,
                "os": os_raw,
                "dotenv": KV(**(custom_fields | dotenv_raw)),
                "secrets": KV(**secrets_raw),   # <- fixed key
            }

        return aggregate, init_settings, env_settings, dotenv_settings, file_secret_settings


@lru_cache(maxsize=1)
def get_env() -> Environment:
    return Environment()

def reload_env() -> None:
    get_env.cache_clear()

# convenient module-level instance
environment: Environment = get_env()
init: dict = get_env().init
os: dict = get_env().os
dotenv: KV = get_env().dotenv
secrets: KV = get_env().secrets

if __name__ == "__main__":
    print(f'Environment.model_dump(): {environment.model_dump()}')
    print(f'dotenv: {dotenv}')
    print(f'dotenv.rootmarker: {dotenv.rootmarker}')
    print(f'secrets: {secrets}')

