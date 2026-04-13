import abc
import json
import os
import re
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import toml
from pydantic import BaseModel, ConfigDict
from toolkit.manager.client import ManagerClient
from toolkit.manager.models import Ixmp4Instance

from ixmp4.core.exceptions import (
    ImproperlyConfigured,
    PlatformNotFound,
    PlatformNotUnique,
)

_ENV_TOKEN_PATTERN = re.compile(r"\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


def resolve_dsn_env_tokens(dsn: str) -> str:
    """Replace {env:VAR_NAME} placeholders with environment variable values."""

    missing: set[str] = set()

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = os.getenv(key)
        if value is None:
            missing.add(key)
            return match.group(0)
        return value

    resolved = _ENV_TOKEN_PATTERN.sub(replace, dsn)
    if missing:
        raise ImproperlyConfigured(
            "Cannot resolve DSN environment variable placeholder(s)."
        )
    return resolved


@runtime_checkable
class PlatformConnectionInfo(Protocol):
    id: int
    name: str
    slug: str

    access_group: int
    management_group: int
    accessibility: str

    dsn: str
    url: Any


class PlatformConnections(abc.ABC):
    @abc.abstractmethod
    def list_platforms(self) -> list[PlatformConnectionInfo]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_platform(self, name: str) -> PlatformConnectionInfo:
        raise NotImplementedError


class TomlPlatform(BaseModel):
    id: int = -1
    name: str
    slug: str

    access_group: int = -1
    management_group: int = -1
    accessibility: str = "PUBLIC"

    dsn: str
    url: str | None = None

    model_config = ConfigDict(from_attributes=True)


class TomlPlatforms(PlatformConnections):
    platforms: dict[str, TomlPlatform]

    def __init__(self, toml_file: Path) -> None:
        self.path = toml_file
        self.load()

    def load(self) -> None:
        dict_ = toml.load(self.path)
        list_: list[dict[str, Any]] = [
            {"name": k, "slug": k, **v} for k, v in dict_.items()
        ]
        self.platforms = {x["name"]: TomlPlatform(**x) for x in list_}

    def dump(self) -> None:
        obj = {}
        for c in self.platforms.values():
            dict_ = json.loads(c.model_dump_json(exclude_unset=True))
            name = dict_.pop("name")
            del dict_["slug"]
            obj[name] = dict_

        f = self.path.open("w+")
        toml.dump(obj, f)

    def list_platforms(self) -> list[TomlPlatform]:
        return [
            platform.model_copy(update={"dsn": resolve_dsn_env_tokens(platform.dsn)})
            for platform in self.platforms.values()
        ]

    def get_platform(self, name: str) -> TomlPlatform:
        try:
            platform = self.platforms[name]
            return platform.model_copy(
                update={"dsn": resolve_dsn_env_tokens(platform.dsn)}
            )
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{name}' was not found.") from e

    def add_platform(self, name: str, dsn: str) -> None:
        try:
            self.get_platform(name)
        except PlatformNotFound:
            self.platforms[name] = TomlPlatform(name=name, slug=name, dsn=dsn)
            self.dump()
            return
        raise PlatformNotUnique(f"Platform '{name}' already exists, remove it first.")

    def remove_platform(self, name: str) -> None:
        try:
            del self.platforms[name]
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{name}' was not found.") from e
        self.dump()


class ManagerPlatforms(PlatformConnections):
    manager_client: ManagerClient

    def __init__(self, manager_client: ManagerClient):
        self.manager_client = manager_client

    def list_platforms(self) -> list[Ixmp4Instance]:
        return [
            platform.model_copy(update={"dsn": resolve_dsn_env_tokens(platform.dsn)})
            for platform in self.manager_client.ixmp4.cached_list()
        ]

    def get_platform(self, name: str) -> Ixmp4Instance:
        for platform in self.manager_client.ixmp4.cached_list():
            if platform.slug == name:
                return platform.model_copy(
                    update={"dsn": resolve_dsn_env_tokens(platform.dsn)}
                )
        else:
            raise PlatformNotFound(f"Platform '{name}' was not found.")
