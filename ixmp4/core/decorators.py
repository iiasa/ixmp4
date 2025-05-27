import functools
from collections.abc import Callable
from typing import Any, TypeVar

import pandera as pa
from pandera.errors import SchemaError as PanderaSchemaError
from pandera.typing import DataFrame

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data.abstract.base import BaseRepository

from .exceptions import SchemaError

T = TypeVar("T")


def check_types(
    func: Callable[[BaseRepository, DataFrame[T]], None],
) -> Callable[[BaseRepository, DataFrame[T]], None]:
    checked_func = pa.check_types(func)

    @functools.wraps(func)
    def wrapper(
        *args: Unpack[tuple[BaseRepository, DataFrame[T]]],
        skip_validation: bool = False,
        **kwargs: Any,
    ) -> None:
        if skip_validation:
            return func(*args, **kwargs)
        try:
            return checked_func(*args, **kwargs)
        except PanderaSchemaError as e:
            raise SchemaError(*e.args)

    return wrapper
