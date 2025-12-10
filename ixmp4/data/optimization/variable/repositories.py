from toolkit import db

from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Variable, VariableIndexsetAssociation, VariableVersion
from .exceptions import (
    VariableDataInvalid,
    VariableNotFound,
    VariableNotUnique,
)
from .filter import VariableFilter


class ItemRepository(IndexedRepository[VariableIndexsetAssociation]):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    DataInvalid = VariableDataInvalid

    target = db.r.ModelTarget(Variable)
    association_target = db.r.ModelTarget(VariableIndexsetAssociation)
    filter = db.r.Filter(VariableFilter, Variable)

    extra_data_columns = {"levels", "marginals"}

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            VariableIndexsetAssociation.variable__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(db.r.ItemRepository[VariableIndexsetAssociation]):
    target = db.r.ModelTarget(VariableIndexsetAssociation)


class PandasRepository(db.r.PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(Variable)
    filter = db.r.Filter(VariableFilter, Variable)


class VersionRepository(db.r.PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(VariableVersion)
    filter = db.r.Filter(VariableFilter, VariableVersion)
