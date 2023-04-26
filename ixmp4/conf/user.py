import pydantic


class User(pydantic.BaseModel):
    id: int | None = None
    username: str
    email: str | None = None
    is_staff: bool = False
    is_superuser: bool = False
    is_verified: bool = False
    is_authenticated: bool = True
    groups: list[int] = []
    jti: str | None = None


local_user = User(
    username="@unknown", is_staff=True, is_superuser=True, is_verified=True
)

anonymous_user = User(username="@anonymous", is_authenticated=False)
