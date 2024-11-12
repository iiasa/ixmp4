from collections.abc import Callable


def autodoc(f: Callable) -> None:
    funcname = f""":func:`{f.__module__}.{f.__qualname__}`\n\n"""
    f.__doc__ = funcname + f.__doc__ if f.__doc__ is not None else funcname
