from toolkit import db

from ixmp4.rewrite.data.optimization.base.repositories import IndexedRepository
from ixmp4.rewrite.exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)

from .db import Equation, EquationIndexsetAssociation, EquationVersion
from .filter import EquationFilter


@registry.register()
class EquationNotFound(NotFound):
    pass


@registry.register()
class EquationNotUnique(NotUnique):
    pass


@registry.register()
class EquationDeletionPrevented(DeletionPrevented):
    pass


@registry.register()
class EquationDataInvalid(OptimizationDataValidationError):
    pass


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
