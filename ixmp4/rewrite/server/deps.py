from contextlib import contextmanager
from typing import AsyncGenerator, Generator

from fastapi import Depends, Header, Path
from sqlalchemy import orm
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from toolkit.token import PreencodedToken, verify

from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.conf.platforms import (
    ManagerPlatforms,
    PlatformConnectionInfo,
    PlatformNotFound,
    TomlPlatform,
)
from ixmp4.rewrite.exceptions import BadRequest, Forbidden
from ixmp4.rewrite.transport import (
    AuthorizedTransport,
    DirectTransport,
    Session,
    cached_create_engine,
)

manager_client = settings.get_manager_client()
manager_platforms = ManagerPlatforms(manager_client)
toml_platforms = settings.get_toml_platforms()


async def validate_token(
    authorization: str | None = Header(None),
) -> PreencodedToken | None:
    """Validates a JSON Web Token with the secret from `settings.secret_hs256`."""

    if authorization is None:
        return None  # anonymous user

    try:
        bearer, encoded_jwt = authorization.split(" ")
    except ValueError:
        raise BadRequest(
            "Invalid 'Authorization' header. "
            "Make sure its value has the format of: 'Bearer <token>'"
        )

    return verify(encoded_jwt, settings.secret_hs256)


async def auth_ctx(
    token: PreencodedToken | None = Depends(validate_token),
) -> AuthorizationContext:
    user = getattr(token, "user", None)
    return AuthorizationContext(user, manager_client)


async def get_manager_platform(
    platform: str = Path(),
    auth_ctx: AuthorizationContext = Depends(auth_ctx),
) -> Ixmp4Instance:
    ci = manager_platforms.get_platform(platform)

    if ci.dsn.startswith("http"):
        raise PlatformNotFound(f"Platform '{ci.name}' was not found.")

    auth_ctx.has_access(
        ci,
        raise_exc=Forbidden(
            f"Access to platform '{platform}' denied due to insufficient permissions."
        ),
    )

    return ci


async def get_toml_platform(
    platform: str = Path(include_in_schema=False),
) -> TomlPlatform:
    ci = toml_platforms.get_platform(platform)

    if ci.dsn.startswith("http"):
        raise PlatformNotFound(f"Platform '{ci.name}' was not found.")

    return ci


@contextmanager
def get_session(
    platform_ci: PlatformConnectionInfo,
) -> Generator[orm.Session, None, None]:
    engine = cached_create_engine(platform_ci.dsn)
    try:
        session = Session(bind=engine)
        yield session
    finally:
        session.rollback()
        session.close()


async def get_manager_session(
    platform_ci: PlatformConnectionInfo = Depends(get_manager_platform),
) -> AsyncGenerator[orm.Session, None]:
    with get_session(platform_ci) as session:
        yield session


async def get_toml_session(
    platform_ci: PlatformConnectionInfo = Depends(get_toml_platform),
) -> AsyncGenerator[orm.Session, None]:
    with get_session(platform_ci) as session:
        yield session


async def get_direct_manager_transport(
    session: orm.Session = Depends(get_manager_session),
) -> DirectTransport:
    return DirectTransport(session)


async def get_direct_toml_transport(
    session: orm.Session = Depends(get_toml_session),
) -> DirectTransport:
    return DirectTransport(session)


async def get_authorized_transport(
    auth_ctx: AuthorizationContext | None = Depends(auth_ctx),
    platform: Ixmp4Instance = Depends(get_manager_platform),
    session: orm.Session = Depends(get_manager_session),
) -> AuthorizedTransport:
    return AuthorizedTransport(session, auth_ctx, platform)
