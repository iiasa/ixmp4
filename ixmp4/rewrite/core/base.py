from ixmp4.rewrite.backend import Backend


class BaseFacade(object):
    _backend: Backend

    def __init__(self, backend: Backend) -> None:
        self._backend = backend
