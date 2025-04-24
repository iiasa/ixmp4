from collections.abc import Callable
from typing import Any


def autodoc(f: Callable[..., Any]) -> Callable[..., Any]:
    funcname = f""":func:`{f.__module__}.{f.__qualname__}`\n\n"""
    f.__doc__ = funcname + f.__doc__ if f.__doc__ is not None else funcname
    return f
