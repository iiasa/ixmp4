from typing import Protocol

import pydantic


class PlatformInfo(pydantic.BaseModel):
    name: str
    dsn: str


class Config(Protocol):
    def list_platforms(self) -> list[PlatformInfo]: ...

    def get_platform(self, key: str) -> PlatformInfo: ...
