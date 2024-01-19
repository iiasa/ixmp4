from datetime import datetime
from typing import ClassVar

from ixmp4.core.base import BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Run
from ixmp4.data.abstract import Table as TableModel


class Table(BaseModelFacade):
    _model: TableModel
    _run: Run
    NotFound: ClassVar = TableModel.NotFound
    NotUnique: ClassVar = TableModel.NotUnique

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def run(self):
        return self._run

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self):
        try:
            return self.backend.optimization.tables.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.optimization.tables.docs.delete(self.id)
        else:
            self.backend.optimization.tables.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.optimization.tables.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Table {self.id} name={self.name}>"
