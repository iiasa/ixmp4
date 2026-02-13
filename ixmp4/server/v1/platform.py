from datetime import datetime, timezone
from typing import Any

import pydantic as pyd
from litestar import Controller, Request, Response, get
from litestar.datastructures import State
from toolkit.auth.context import AuthorizationContext
from toolkit.auth.user import User

from ixmp4._version import __version__ as __version__
from ixmp4.conf.platforms import (
    PlatformConnectionInfo,
)


class PlatformInfo(pyd.BaseModel):
    slug: str
    name: str
    version: str
    is_managed: bool
    manager_url: None | str
    utcnow: datetime


class PlatformAuthStatus(pyd.BaseModel):
    access: bool
    view: bool
    submit: bool
    edit: bool
    manage: bool


class PlatformStatus(pyd.BaseModel):
    auth: PlatformAuthStatus | None = None


class PlatformController(Controller):
    @get("/")
    async def info(
        self,
        platform: PlatformConnectionInfo,
        state: State,
    ) -> Response[PlatformInfo]:
        return Response(
            PlatformInfo(
                slug=platform.slug,
                name=platform.name,
                version=__version__,
                is_managed=state.settings.manager_url is not None,
                manager_url=state.settings.manager_url,
                utcnow=datetime.now(tz=timezone.utc),
            )
        )

    @get("/status")
    async def status(
        self,
        platform: PlatformConnectionInfo,
        request: Request[User | None, AuthorizationContext | None, Any],
    ) -> Response[PlatformStatus]:
        status = PlatformStatus()
        if request.auth is not None:
            status.auth = PlatformAuthStatus(
                access=request.auth.has_access_permission(platform),
                view=request.auth.has_view_permission(platform),
                submit=request.auth.has_submit_permission(platform),
                edit=request.auth.has_edit_permission(platform),
                manage=request.auth.has_management_permission(platform),
            )
        return Response(status)
