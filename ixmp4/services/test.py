from typing import AsyncGenerator

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext
from toolkit.client.auth import SelfSignedAuth
from toolkit.manager import ManagerClient
from toolkit.manager.models import User

from ixmp4.conf import settings
from ixmp4.services.region import RegionService

engine = sa.create_engine("sqlite://")


async def session_dep() -> AsyncGenerator[orm.Session, None]:
    with engine.connect() as conn:
        with orm.Session(conn) as session:
            yield session


async def auth_dep() -> AuthorizationContext:
    auth = SelfSignedAuth(settings.secret_hs256)
    manager_client = ManagerClient(str(settings.manager_url), auth)
    user = User(
        id=-1,
        username="god",
        email="god@god.com",
        is_staff=True,
        is_superuser=True,
        is_verified=True,
        is_authenticated=True,
        groups=[],
    )

    return AuthorizationContext(user, manager_client)


router = RegionService.build_router(session_dep, auth_dep)
pass
