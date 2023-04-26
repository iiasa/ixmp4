def autodoc(f):
    funcname = f""":func:`{f.__module__}.{f.__qualname__}`\n\n"""
    if f.__doc__ is not None:
        f.__doc__ = funcname + f.__doc__
    else:
        f.__doc__ = funcname
