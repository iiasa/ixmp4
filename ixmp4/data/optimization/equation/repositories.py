from toolkit import db

from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Equation, EquationIndexsetAssociation, EquationVersion
from .exceptions import EquationDataInvalid, EquationNotFound, EquationNotUnique
from .filter import EquationFilter


class ItemRepository(IndexedRepository[Equation, EquationIndexsetAssociation]):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    DataInvalid = EquationDataInvalid
    target = db.r.ModelTarget(Equation)
    association_target = db.r.ModelTarget(EquationIndexsetAssociation)
    filter = db.r.Filter(EquationFilter, Equation)
    extra_data_columns = {"levels", "marginals"}

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            EquationIndexsetAssociation.equation__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(db.r.ItemRepository[EquationIndexsetAssociation]):
    target = db.r.ModelTarget(EquationIndexsetAssociation)


class PandasRepository(db.r.PandasRepository):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    target = db.r.ModelTarget(Equation)
    filter = db.r.Filter(EquationFilter, Equation)


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    target = db.r.ModelTarget(EquationVersion)
    filter = db.r.Filter(EquationFilter, EquationVersion)
