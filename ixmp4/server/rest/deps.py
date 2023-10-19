import logging
from typing import AsyncGenerator, Callable

import jwt
from fastapi import Depends, Header, Path

from ixmp4.conf import settings
from ixmp4.conf.auth import SelfSignedAuth
from ixmp4.conf.manager import ManagerConfig
from ixmp4.conf.user import User, anonymous_user, local_user
from ixmp4.core.exceptions import Forbidden, InvalidToken, PlatformNotFound
from ixmp4.data.backend.db import SqlAlchemyBackend

logger = logging.getLogger(__name__)
manager = ManagerConfig(
    str(settings.manager_url), SelfSignedAuth(settings.secret_hs256)
)


async def validate_token(authorization: str = Header(None)) -> dict | None:
    """Validates a JSON Web Token with the secret supplied in the
    `IXMP4_SECRET_HS256` environment variable."""

    if authorization is None:
        return None  # anonymous user

    encoded_jwt = authorization.split(" ")[1]
    try:
        return jwt.decode(
            encoded_jwt, settings.secret_hs256, leeway=300, algorithms=["HS256"]
        )
    except jwt.InvalidTokenError as e:
        raise InvalidToken("The supplied token is expired or invalid.") from e


async def do_not_validate_token(authorization: str = Header(None)) -> dict | None:
    """Override dependency used for skipping authentication while testing."""
    return {"user": local_user.model_dump()}


async def get_user(token: dict | None = Depends(validate_token)) -> User:
    """Returns a user object for permission checks."""
    if token is None:
        return anonymous_user

    try:
        user_dict = token["user"]
        user_dict["jti"] = token.get("jti", None)
    except KeyError as e:
        raise InvalidToken("The supplied token is malformed.") from e

    return User(**user_dict)


async def get_version():
    from ixmp4 import __version__

    return __version__


async def get_managed_backend(
    platform: str = Path(), user: User = Depends(get_user)
) -> AsyncGenerator[SqlAlchemyBackend, None]:
    """Returns a platform backend for a platform name as a path parameter.
    Also checks user access permissions if in managed mode."""
    info = manager.get_platform(platform, jti=manager.auth.get_user().jti)

    if info.dsn.startswith("http"):
        raise PlatformNotFound(f"Platform '{platform}' was not found.")
    else:
        backend = SqlAlchemyBackend(info)
        with backend.auth(user, manager, info) as auth:
            if auth.is_accessible:
                yield backend
            else:
                raise Forbidden(
                    f"Access to platform '{platform}' denied "
                    "due to insufficient permissions."
                )
        backend.close()


async def get_toml_backend(
    platform: str = Path(), user: User = Depends(get_user)
) -> AsyncGenerator[SqlAlchemyBackend, None]:
    logger.debug("Looking for platform in `platforms.toml`.")
    info = settings.toml.get_platform(platform)
    if info.dsn.startswith("http"):
        raise PlatformNotFound(f"Platform '{platform}' was not found.")
    else:
        backend = SqlAlchemyBackend(info)
        yield backend
        backend.close()


if settings.managed:
    get_backend = get_managed_backend
else:
    get_backend = get_toml_backend


def get_test_backend_dependency(backend, auth_params) -> Callable:
    async def get_memory_backend(
        platform: str = Path(), user: User = Depends(get_user)
    ) -> AsyncGenerator[SqlAlchemyBackend, None]:
        """Override dependency which always yields a test backend."""
        with backend.auth(*auth_params, overlap_ok=True):
            yield backend

    return get_memory_backend
