import logging
from datetime import datetime, timedelta
from uuid import uuid4

import httpx
import jwt

from ixmp4.core.exceptions import InvalidCredentials, IxmpError

from .user import User, anonymous_user

logger = logging.getLogger(__name__)


class BaseAuth(object):
    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def auth_flow(self, request):
        yield self(request)

    def get_user(self) -> User:
        raise NotImplementedError


class SelfSignedAuth(BaseAuth, httpx.Auth):
    """Generates its own JWT with the supplied secret."""

    def __init__(self, secret: str, username: str = "ixmp4"):
        self.secret = secret
        self.user = User(
            id=-1,
            username=username,
            email="ixmp4@iiasa.ac.at",
            is_staff=True,
            is_superuser=True,
            is_verified=True,
            groups=[],
        )
        self.token = self.get_local_jwt()

    def __call__(self, r):
        try:
            jwt.decode(self.token, self.secret, algorithms=["HS256"])
        except (jwt.InvalidTokenError, jwt.ExpiredSignatureError):
            self.token = self.get_local_jwt()

        r.headers["Authorization"] = "Bearer " + self.token
        return r

    def get_local_jwt(self):
        self.jti = uuid4().hex
        return jwt.encode(
            {
                "token_type": "access",
                "exp": self.get_expiration_timestamp(),
                "jti": self.jti,
                "sub": "ixmp4",
                "iss": "ixmp4",
                "user": self.user.model_dump(),
            },
            self.secret,
            algorithm="HS256",
        )

    def get_expiration_timestamp(self):
        return int((datetime.now() + timedelta(minutes=15)).timestamp())

    def get_user(self) -> User:
        self.user.jti = self.jti
        return self.user


class AnonymousAuth(BaseAuth, httpx.Auth):
    def __init__(self):
        self.user = anonymous_user
        logger.info("Connecting to service anonymously and without credentials.")

    def __call__(self, r):
        return r

    def get_user(self) -> User:
        return self.user


class ManagerAuth(BaseAuth, httpx.Auth):
    """Uses the SceSe AAC/Management Service to obtain and refresh a token."""

    def __init__(
        self,
        username: str,
        password: str,
        url: str,
    ):
        self.client = httpx.Client(base_url=url, timeout=10.0, http2=True)
        self.username = username
        self.password = password
        self.obtain_jwt()

    def __call__(self, r):
        try:
            jwt.decode(
                self.access_token,
                options={"verify_signature": False, "verify_exp": True},
            )
        except jwt.ExpiredSignatureError:
            self.refresh_or_reobtain_jwt()

        r.headers["Authorization"] = "Bearer " + self.access_token
        return r

    def obtain_jwt(self):
        res = self.client.post(
            "/token/obtain/",
            json={
                "username": self.username,
                "password": self.password,
            },
        )
        if res.status_code >= 400:
            if res.status_code == 401:
                raise InvalidCredentials(
                    "Your credentials were rejected by the "
                    "Scenario Services Management System. "
                    "Check if they are correct and your account is active "
                    "or log out with `ixmp4 logout` to use ixmp4 anonymously."
                )
            else:
                raise IxmpError("Unknown API error: " + res.text)

        json = res.json()
        self.access_token = json["access"]
        self.set_user(self.access_token)
        self.refresh_token = json["refresh"]

    def refresh_or_reobtain_jwt(self):
        try:
            jwt.decode(
                self.refresh_token,
                options={"verify_signature": False, "verify_exp": True},
            )
            self.refresh_jwt()
        except jwt.ExpiredSignatureError:
            self.obtain_jwt()

    def refresh_jwt(self):
        res = self.client.post(
            "/token/refresh/",
            json={
                "refresh": self.refresh_token,
            },
        )

        if res.status_code >= 400:
            raise IxmpError("Unknown API error: " + res.text)

        self.access_token = res.json()["access"]
        self.set_user(self.access_token)

    def decode_token(self, token: str):
        return jwt.decode(
            token,
            options={"verify_signature": False, "verify_exp": False},
        )

    def set_user(self, token: str):
        token_dict = self.decode_token(token)
        user_dict = token_dict["user"]
        self.user = User(**user_dict, jti=token_dict.get("jti"))

    def get_user(self) -> User:
        return self.user
