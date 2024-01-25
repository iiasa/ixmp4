from functools import wraps
from typing import TYPE_CHECKING, Callable, Protocol

from ixmp4.core.exceptions import Forbidden, ProgrammingError

if TYPE_CHECKING:
    from ..backend.db import SqlAlchemyBackend


class Guardable(Protocol):
    # NOTE: Eager checking for api backends may be desirable
    # at some point
    backend: "SqlAlchemyBackend"


def guard(access: str) -> Callable:
    if access not in ["edit", "manage", "view"]:
        raise ProgrammingError("Guard access must be 'edit', 'manage' or 'view'.")

    def decorator(func):
        @wraps(func)
        def guarded_func(self: Guardable, *args, **kwargs):
            if self.backend.auth_context is not None:
                if access == "view" and self.backend.auth_context.is_viewable:
                    return func(self, *args, **kwargs)
                elif access == "edit" and self.backend.auth_context.is_editable:
                    return func(self, *args, **kwargs)
                elif access == "manage" and self.backend.auth_context.is_managed:
                    return func(self, *args, **kwargs)

                raise Forbidden(
                    f"Function '{func.__name__}': '{access}' access denied due to "
                    "insufficient permissions."
                )
            else:
                # ignoring authorization since no auth context is set
                return func(self, *args, **kwargs)

        return guarded_func

    return decorator
