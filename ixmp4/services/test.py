from typing import AsyncGenerator

import fastapi as fa
import sqlalchemy as sa
from fastapi.testclient import TestClient
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext
from toolkit.client.auth import SelfSignedAuth
from toolkit.manager import ManagerClient
from toolkit.manager.models import User

from ixmp4.conf import settings
from ixmp4.services.base import DirectTransport, HttpxTransport
from ixmp4.services.region import Region, RegionService

engine = sa.create_engine(
    "sqlite:///./run/svctest.sqlite", connect_args={"check_same_thread": False}
)
Region.metadata.create_all(engine, checkfirst=True)


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

auth_ctx = AuthorizationContext(user, manager_client)


app = fa.FastAPI()
router = RegionService.build_router(session_dep, auth_dep)
app.include_router(router)
client = TestClient(app)

with engine.connect() as conn:
    with orm.Session(conn) as session:
        direct = RegionService(DirectTransport(session, auth_ctx))
        dregion = direct.get_or_create("direct", "default")
        dregions = direct.list()

http = RegionService(HttpxTransport(client))
hregion = http.get_or_create("http", "default")
hregions = http.list()
dir_regions = http.list(name__like="dir*")

print(dregion)
print(hregion)
