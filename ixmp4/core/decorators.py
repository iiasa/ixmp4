import functools

import pandera as pa
from pandera.errors import SchemaError as PanderaSchemaError

from .exceptions import SchemaError


def check_types(func):
    checked_func = pa.check_types(func)

    @functools.wraps(func)
    def wrapper(*args, skip_validation: bool = False, **kwargs):
        if skip_validation:
            return func(*args, **kwargs)
        try:
            return checked_func(*args, **kwargs)
        except PanderaSchemaError as e:
            raise SchemaError(*e.args)

    return wrapper
