import enum
import httpx
import re
import os
from functools import lru_cache

import pandas as pd
from pydantic import Field

from ixmp4.core.exceptions import PlatformNotFound, ManagerApiError

from .auth import BaseAuth
from .user import User
from .base import Config, PlatformInfo


class hashabledict(dict):
    """Hashable dict type used for caching."""

    def __hash__(self):
        return hash(tuple(sorted(self.items())))


class ManagerPlatformInfo(PlatformInfo):
    id: int
    management_group: int
    access_group: int
    url: str
    name: str = Field(alias="slug")

    class Accessibilty(str, enum.Enum):
        PUBLIC = "PUBLIC"
        GATED = "GATED"
        PRIVATE = "PRIVATE"

    accessibility: Accessibilty


class ManagerConfig(Config):
    template_pattern = re.compile(r"(\{env\:(\w+)\})")

    def __init__(self, url: str, auth: BaseAuth, remote: bool = False) -> None:
        self.url = url
        self.auth = auth
        self.client = httpx.Client(
            base_url=self.url, timeout=10.0, http2=True, auth=auth
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

    # TODO: Find the sweet-spot for `maxsize`
    # -> a trade-off between memory usage
    # and load on the management service
    @lru_cache(maxsize=128)
    def _cached_request(self, *args, jti: str | None = None, **kwargs):
        del jti
        # `jti` is only used to affect `@lru_cache`
        # set to `None` to ignore for now
        # if the token id changes a new cache entry will be created
        # TODO: MU improvement:
        # the old cache entry could be completely invalidated/deleted/collected
        # NOTE: since this cache is not shared amongst processes, it's efficacy
        # declines with the scale of the whole infrastructure unless counteracted
        # with increased cache size / memory usage

        res = self.client.request(*args, **kwargs)
        if res.status_code != 200:
            raise ManagerApiError(f"[{str(res.status_code)}] {res.text}")
        return res.json()

    def _request(
        self,
        *args,
        params: dict | None = None,
        json: dict | list | tuple | None = None,
        **kwargs,
    ):
        if params is not None:
            params = hashabledict(params)

        if json is not None:
            if type(json) == dict:
                json = hashabledict(json)
            else:
                json = tuple(json)

        return self._cached_request(*args, params=params, json=json, **kwargs)

    def fetch_platforms(self, **kwargs) -> list[ManagerPlatformInfo]:
        json = self._request("GET", "/ixmp4", params={"page_size": -1}, **kwargs)
        return [ManagerPlatformInfo(**c) for c in json["results"]]

    def list_platforms(self, **kwargs) -> list[ManagerPlatformInfo]:
        platforms = self.fetch_platforms(**kwargs)

        for i, p in enumerate(platforms):
            if self.remote:
                platforms[i].dsn = p.url
            else:
                platforms[i].dsn = self.expand_dsn(p.dsn)

        return platforms

    def get_platform(self, key: str, **kwargs) -> ManagerPlatformInfo:
        for p in self.list_platforms(**kwargs):
            if p.name == key:
                return p
        else:
            raise PlatformNotFound

    def fetch_user_permissions(
        self, user: User, platform: ManagerPlatformInfo, **kwargs
    ) -> pd.DataFrame:
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
        self, group_id: int, platform: ManagerPlatformInfo, **kwargs
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
        self, user: User, platform: ManagerPlatformInfo, **kwargs
    ) -> pd.DataFrame:
        pdf = self.permissions
        return pdf.where(pdf["group"].isin(user.groups)).where(
            pdf["instance"] == platform.id
        )

    def fetch_group_permissions(
        self, group_id: int, platform: ManagerPlatformInfo, **kwargs
    ) -> pd.DataFrame:
        pdf = self.permissions
        return pdf.where(pdf["group"] == group_id).where(pdf["instance"] == platform.id)
