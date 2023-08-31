from ixmp4.data.abstract import BaseModel
from ixmp4.data.backend import Backend


class BaseFacade(object):
    backend: Backend

    def __init__(self, _backend: Backend | None = None) -> None:
        if _backend is None:
            raise ValueError(
                f"Cannot initialize `{self.__class__.__name__}` without `_backend`."
            )
        self.backend = _backend


class BaseModelFacade(BaseFacade):
    backend: Backend
    _model: BaseModel

    def __init__(self, *args, _model: BaseModel | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if _model is not None:
            self._model = _model
