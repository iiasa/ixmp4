import abc
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Protocol, Sequence, runtime_checkable

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
logger = logging.getLogger(__name__)


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
        logger.debug("Missing DSN environment variable(s): " + str(missing))
        raise ImproperlyConfigured(
            "Cannot resolve DSN environment variable placeholder(s)."
        )
    return resolved


@runtime_checkable
class PlatformConnectionInfo(Protocol):
    """Structural interface for platform connection metadata.

    Attributes
    ----------

    id: int | None
        Manager platform identifier, if known.
    name: str
        Human-readable platform name used as the local key.
    slug: str
        Stable platform slug used for lookups.
    access_group: int | None
        Manager access group identifier, if known.
    management_group: int | None
        Manager management group identifier, if known.
    accessibility: str
        Platform visibility setting.
    dsn: str
        Database connection string for the platform.
    url: str
        Optional server URL associated with the platform.
    """

    @property
    def id(self) -> int | None: ...

    @property
    def name(self) -> str: ...

    @property
    def slug(self) -> str: ...

    @property
    def access_group(self) -> int | None: ...

    @property
    def management_group(self) -> int | None: ...

    @property
    def accessibility(self) -> str: ...

    @property
    def dsn(self) -> str: ...

    @property
    def url(self) -> str | None: ...


class PlatformConnections(abc.ABC):
    """Abstract interface for listing and retrieving platform connections."""

    @abc.abstractmethod
    def list_platforms(self) -> Sequence[PlatformConnectionInfo]:
        """Return all available platform connection definitions."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_platform(self, name: str) -> PlatformConnectionInfo:
        """Return a single platform connection definition by name."""
        raise NotImplementedError


class TomlPlatform(BaseModel):
    """Pydantic model for a platform entry stored in TOML configuration."""

    id: int | None = None
    name: str
    slug: str

    access_group: int | None = None
    management_group: int | None = None
    accessibility: str = "PUBLIC"

    dsn: str
    url: str | None = None

    model_config = ConfigDict(from_attributes=True)


class TomlPlatforms(PlatformConnections):
    """Platform connection registry backed by a local TOML file."""

    platforms: dict[str, TomlPlatform]

    def __init__(self, toml_file: Path) -> None:
        """Initialize the registry from a TOML configuration file."""
        self.path = toml_file
        self.load()

    def load(self) -> None:
        """Load platform definitions from disk into memory."""
        dict_ = toml.load(self.path)
        list_: list[dict[str, Any]] = [
            {"name": k, "slug": k, **v} for k, v in dict_.items()
        ]
        self.platforms = {x["name"]: TomlPlatform(**x) for x in list_}

    def dump(self) -> None:
        """Write the in-memory platform definitions back to disk."""
        obj = {}
        for c in self.platforms.values():
            dict_ = json.loads(c.model_dump_json(exclude_unset=True))
            name = dict_.pop("name")
            del dict_["slug"]
            obj[name] = dict_

        f = self.path.open("w+")
        toml.dump(obj, f)

    def list_platforms(self) -> list[TomlPlatform]:
        """Return all configured platforms from the TOML registry."""
        return list(self.platforms.values())

    def get_platform(self, name: str) -> TomlPlatform:
        """Return one configured platform by name.

        Parameters
        ----------
        name : str
            Platform slug to search for.

        Raises
        ------
        :class:`~ixmp4.base_exceptions.PlatformNotFound`:
            If the platform with `name` does not exist.
        """
        try:
            return self.platforms[name]
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{name}' was not found.") from e

    def add_platform(self, name: str, dsn: str) -> None:
        """Add a new platform entry and persist the updated registry.

        Parameters
        ----------
        name : str
            Slug for the platform to add.
        dsn : str
            Platform dsn connection string or http url.

        Raises
        ------
        :class:`~ixmp4.base_exceptions.PlatformNotUnique`:
            If the platform with `name` already exists.
        """
        try:
            self.get_platform(name)
        except PlatformNotFound:
            self.platforms[name] = TomlPlatform(name=name, slug=name, dsn=dsn)
            self.dump()
            return
        raise PlatformNotUnique(f"Platform '{name}' already exists, remove it first.")

    def remove_platform(self, name: str) -> None:
        """Remove a platform entry and persist the updated registry.

        Parameters
        ----------
        name : str
            Slug for the platform to delete.

        Raises
        ------
        :class:`~ixmp4.base_exceptions.PlatformNotFound`:
            If the platform with `name` does not exist.
        """
        try:
            del self.platforms[name]
        except KeyError as e:
            raise PlatformNotFound(f"Platform '{name}' was not found.") from e
        self.dump()


class ManagerPlatforms(PlatformConnections):
    """Platform connection registry backed by the manager service API."""

    manager_client: ManagerClient

    def __init__(self, manager_client: ManagerClient):
        """Initialize the registry with a manager API client."""
        self.manager_client = manager_client

    def list_platforms(self) -> list[Ixmp4Instance]:
        """Return all platforms visible from the manager service."""
        return self.manager_client.ixmp4.cached_list()

    def get_platform(self, name: str) -> Ixmp4Instance:
        """Return one manager platform by slug.

        Parameters
        ----------
        name : str
            Platform slug to search for.

        Raises
        ------
        :class:`~ixmp4.base_exceptions.PlatformNotFound`:
            If the platform with `name` does not exist.
        """
        for platform in self.manager_client.ixmp4.cached_list():
            if platform.slug == name:
                return platform
        raise PlatformNotFound(f"Platform '{name}' was not found.")
