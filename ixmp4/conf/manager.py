import enum
import logging
import os
import re
from functools import lru_cache
from typing import Any, cast

import httpx
import pandas as pd
from pydantic import Field

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4.core.exceptions import ManagerApiError, PlatformNotFound

from .auth import BaseAuth
from .base import Config, PlatformInfo
from .user import User

logger = logging.getLogger(__name__)


class hashabledict(dict[str, Any]):
    """Hashable dict type used for caching."""

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.items())))


class ManagerPlatformInfo(PlatformInfo):
    id: int
    management_group: int
    access_group: int
    url: str
    name: str = Field(alias="slug")
    notice: str | None = Field(default=None)

    class Accessibilty(str, enum.Enum):
        PUBLIC = "PUBLIC"
        GATED = "GATED"
        PRIVATE = "PRIVATE"

    accessibility: Accessibilty


class JtiKwargs(TypedDict, total=False):
    jti: str | None


class ManagerConfig(Config):
    template_pattern = re.compile(r"(\{env\:(\w+)\})")

    def __init__(self, url: str, auth: BaseAuth | None, remote: bool = False) -> None:
        # TODO: Find the sweet-spot for `maxsize`
        # -> a trade-off between memory usage
        # and load on the management service

        self._cached_request = lru_cache(maxsize=128)(self._uncached_request)
        self.url = url
        self.auth = auth
        self.client = httpx.Client(
            base_url=self.url,
            timeout=10.0,
            http2=True,
            auth=auth,
            follow_redirects=True,
        )
        self.remote = remote

    def expand_dsn(self, dsn: str) -> str:
        for template, var in self.template_pattern.findall(dsn):
            if not var.startswith("IXMP4_"):
                # TODO: logging: warn/error if variable name is not valid
                continue
            try:
                val = os.environ[var]
            except KeyError:
                # TODO: logging: warn/error if variable is not in environment
                continue
            dsn = dsn.replace(template, val)
        return dsn

    def _uncached_request(
        self,
        method: str,
        path: str,
        params: dict[str, int | None] | None = None,
        json: dict[str, Any] | list[Any] | tuple[Any] | None = None,
        jti: str | None = None,
    ) -> dict[str, Any]:
        del jti
        # `jti` is only used to affect `@lru_cache`
        # if the token id changes a new cache entry will be created
        # TODO: MU improvement:
        # the old cache entry could be completely invalidated/deleted/collected
        # NOTE: since this cache is not shared amongst processes, it's efficacy
        # declines with the scale of the whole infrastructure unless counteracted
        # with increased cache size / memory usage
        res = self.client.request(method, path, params=params, json=json)
        if res.status_code != 200:
            raise ManagerApiError(f"[{str(res.status_code)}] {res.text}")
        # NOTE we can assume this type, might get replaced with scse-toolkit
        return cast(dict[str, Any], res.json())

    def _request(
        self,
        method: str,
        path: str,
        # Seems to be just that based on references
        params: dict[str, int | None] | None = None,
        # Seems to not be included with any references?
        json: dict[str, Any] | list[Any] | tuple[Any] | None = None,
        **kwargs: Unpack[JtiKwargs],
    ) -> dict[str, Any]:
        if params is not None:
            params = hashabledict(params)

        if json is not None:
            json = hashabledict(json) if isinstance(json, dict) else tuple(json)

        logger.debug(f"Trying cache: {method} {path} {params} {json}")
        return self._cached_request(method, path, params=params, json=json, **kwargs)

    def fetch_platforms(self, **kwargs: Unpack[JtiKwargs]) -> list[ManagerPlatformInfo]:
        json = self._request("GET", "/ixmp4", params={"page_size": -1}, **kwargs)
        return [ManagerPlatformInfo(**c) for c in json["results"]]

    def list_platforms(self, **kwargs: Unpack[JtiKwargs]) -> list[ManagerPlatformInfo]:
        platforms = self.fetch_platforms(**kwargs)

        for i, p in enumerate(platforms):
            platforms[i].dsn = p.url if self.remote else self.expand_dsn(p.dsn)

        return platforms

    def get_platform(
        self, key: str, **kwargs: Unpack[JtiKwargs]
    ) -> ManagerPlatformInfo:
        for p in self.list_platforms(**kwargs):
            if p.name == key:
                return p
        else:
            raise PlatformNotFound(
                f"Platform '{key}' does not exist at {self.url} or "
                "you do not have permission to access this platform."
            )

    def fetch_user_permissions(
        self,
        user: User,
        platform: ManagerPlatformInfo,
        **kwargs: Unpack[JtiKwargs],
    ) -> pd.DataFrame:
        if not user.is_authenticated:
            return pd.DataFrame(
                [], columns=["id", "instance", "group", "access_type", "model"]
            )
        json = self._request(
            "GET",
            "/modelpermissions",
            params={
                "page_size": -1,
                "instance": platform.id,
                "group__users": user.id,
            },
            **kwargs,
        )
        return pd.DataFrame(
            json["results"], columns=["id", "instance", "group", "access_type", "model"]
        )

    def fetch_group_permissions(
        self,
        group_id: int,
        platform: ManagerPlatformInfo,
        **kwargs: Unpack[JtiKwargs],
    ) -> pd.DataFrame:
        json = self._request(
            "GET",
            "/modelpermissions",
            params={"page_size": -1, "instance": platform.id, "group": group_id},
            **kwargs,
        )
        return pd.DataFrame(
            json["results"], columns=["id", "instance", "group", "access_type", "model"]
        )


class MockManagerConfig(ManagerConfig):
    def __init__(
        self,
        platforms: list[ManagerPlatformInfo],
        permissions: pd.DataFrame,
    ) -> None:
        self.platforms = platforms
        self.permissions = permissions
        self.remote = False

    def fetch_platforms(self) -> list[ManagerPlatformInfo]:
        return self.platforms

    def fetch_user_permissions(
        self,
        user: User,
        platform: ManagerPlatformInfo,
        **kwargs: Unpack[JtiKwargs],
    ) -> pd.DataFrame:
        pdf = self.permissions
        return pdf.where(pdf["group"].isin(user.groups)).where(
            pdf["instance"] == platform.id
        )

    def fetch_group_permissions(
        self,
        group_id: int,
        platform: ManagerPlatformInfo,
        **kwargs: Unpack[JtiKwargs],
    ) -> pd.DataFrame:
        pdf = self.permissions
        return pdf.where(pdf["group"] == group_id).where(pdf["instance"] == platform.id)
