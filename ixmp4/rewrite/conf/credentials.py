from contextlib import suppress
from pathlib import Path
from typing import TypedDict

import toml


class CredentialsDict(TypedDict):
    username: str
    password: str


class Credentials(object):
    credentials: dict[str, CredentialsDict]

    def __init__(self, toml_file: Path) -> None:
        self.path = toml_file
        self.load()

    def load(self) -> None:
        self.credentials = toml.load(self.path)

    def dump(self) -> None:
        f = self.path.open("w+")
        toml.dump(self.credentials, f)

    def get(self, key: str) -> CredentialsDict | None:
        return self.credentials.get(key, None)

    def set(self, key: str, username: str, password: str) -> None:
        self.credentials[key] = {
            "username": username,
            "password": password,
        }
        self.dump()

    def clear(self, key: str) -> None:
        with suppress(KeyError):
            del self.credentials[key]
        self.dump()
