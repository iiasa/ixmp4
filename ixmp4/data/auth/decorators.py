from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar

from ixmp4.core.exceptions import Forbidden, ProgrammingError

if TYPE_CHECKING:
    from ..db.base import BaseModel, BaseRepository

P = ParamSpec("P")

ReturnT = TypeVar("ReturnT")


def guard(
    access: str,
) -> Callable[
    [Callable[Concatenate[Any, P], ReturnT]], Callable[Concatenate[Any, P], ReturnT]
]:
    if access not in ["edit", "manage", "view"]:
        raise ProgrammingError("Guard access must be 'edit', 'manage' or 'view'.")

    def decorator(
        func: Callable[Concatenate[Any, P], ReturnT],
    ) -> Callable[Concatenate[Any, P], ReturnT]:
        @wraps(func)
        def guarded_func(
            self: "BaseRepository[BaseModel]", /, *args: P.args, **kwargs: P.kwargs
        ) -> ReturnT:
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
