import json
from pathlib import Path
from typing import Any

import toml

from ixmp4.core.exceptions import PlatformNotFound, PlatformNotUnique

from .base import Config, PlatformInfo
from .user import User


class TomlPlatformInfo(PlatformInfo):
    pass


class TomlConfig(Config):
    platforms: dict[str, TomlPlatformInfo]

    def __init__(self, toml_file: Path, user: User) -> None:
        self.path = toml_file
        self.user = user
        self.load()

    def load(self) -> None:
        dict_ = toml.load(self.path)
        list_: list[dict[str, Any]] = [{"name": k, **v} for k, v in dict_.items()]
        self.platforms = {x["name"]: TomlPlatformInfo(**x) for x in list_}

    def dump(self):
        obj = {}
        for c in self.platforms.values():
            dict_ = json.loads(c.model_dump_json())
            dict_.pop("user", None)
            name = dict_.pop("name")
            obj[name] = dict_

        f = self.path.open("w+")
        toml.dump(obj, f)

    def list_platforms(self) -> list[TomlPlatformInfo]:
        return list(self.platforms.values())

    def get_platform(self, key: str) -> TomlPlatformInfo:
        try:
            return self.platforms[key]
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{key}' was not found.") from e

    def add_platform(self, name: str, dsn: str):
        try:
            self.get_platform(name)
        except PlatformNotFound:
            self.platforms[name] = TomlPlatformInfo(name=name, dsn=dsn)
            self.dump()
            return
        raise PlatformNotUnique(f"Platform '{name}' already exists, remove it first.")

    def remove_platform(self, key: str):
        try:
            del self.platforms[key]
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{key}' was not found.") from e
        self.dump()
