import abc
import json
from pathlib import Path
from typing import Any, Protocol, Sequence

import toml
from pydantic import BaseModel, ConfigDict
from toolkit.manager.client import ManagerClient
from toolkit.manager.models import Ixmp4Instance

from ixmp4.rewrite.exceptions import PlatformNotFound, PlatformNotUnique


class PlatformConnectionInfo(Protocol):
    name: str
    dsn: str
    url: Any


class PlatformConnections(abc.ABC):
    @abc.abstractmethod
    def list_platforms(self) -> list[PlatformConnectionInfo]:
        pass

    @abc.abstractmethod
    def get_platform(self, name: str) -> PlatformConnectionInfo:
        pass


class TomlPlatform(BaseModel):
    name: str
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
        list_: list[dict[str, Any]] = [{"name": k, **v} for k, v in dict_.items()]
        self.platforms = {x["name"]: TomlPlatform(**x) for x in list_}

    def dump(self) -> None:
        obj = {}
        for c in self.platforms.values():
            dict_ = json.loads(c.model_dump_json())
            name = dict_.pop("name")
            obj[name] = dict_

        f = self.path.open("w+")
        toml.dump(obj, f)

    def list_platforms(self) -> Sequence[TomlPlatform]:
        return list(self.platforms.values())

    def get_platform(self, name: str) -> TomlPlatform:
        try:
            return self.platforms[name]
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{name}' was not found.") from e

    def add_platform(self, name: str, dsn: str) -> None:
        try:
            self.get_platform(name)
        except PlatformNotFound:
            self.platforms[name] = TomlPlatform(name=name, dsn=dsn)
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

    def list_platforms(self) -> Sequence[Ixmp4Instance]:
        return self.manager_client.ixmp4.cached_list()

    def get_platform(self, name: str) -> Ixmp4Instance:
        ixmp4_inst = self.manager_client.ixmp4.cached_list()
        for i in ixmp4_inst:
            if i.name == name:
                return i
        else:
            raise PlatformNotFound(f"Platform '{name}' was not found.")
