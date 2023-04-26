from ..base import BaseFacade
from .variable import VariableRepository


class IamcRepository(BaseFacade):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.variables = VariableRepository(_backend=self.backend)
