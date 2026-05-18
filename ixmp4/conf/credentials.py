from contextlib import suppress
from pathlib import Path

import toml
from typing_extensions import TypedDict


class CredentialsDict(TypedDict):
    """Stored credential fields for a single named service entry."""

    username: str
    password: str


class Credentials(object):
    """Credential store backed by a local TOML file.

    Attributes
    ----------

    credentials: Mapping of named entries to username/password pairs.
    """

    credentials: dict[str, CredentialsDict]

    def __init__(self, toml_file: Path) -> None:
        """Initialize the credential store from a TOML file."""
        self.path = toml_file
        self.load()

    def load(self) -> None:
        """Load credentials from disk into memory."""
        self.credentials = toml.load(self.path)

    def dump(self) -> None:
        """Write the in-memory credentials back to disk."""
        f = self.path.open("w+")
        toml.dump(self.credentials, f)

    def get(self, key: str) -> CredentialsDict | None:
        """Return stored credentials for a named entry, if present."""
        return self.credentials.get(key, None)

    def set(self, key: str, username: str, password: str) -> None:
        """Store credentials for a named entry and persist the change."""
        self.credentials[key] = {
            "username": username,
            "password": password,
        }
        self.dump()

    def clear(self, key: str) -> None:
        """Remove stored credentials for a named entry if it exists."""
        with suppress(KeyError):
            del self.credentials[key]
        self.dump()
