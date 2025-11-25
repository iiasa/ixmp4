from toolkit import db

from ixmp4.rewrite.data.optimization.base.repositories import IndexedRepository
from ixmp4.rewrite.exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)

from .db import Parameter, ParameterIndexsetAssociation, ParameterVersion
from .filter import ParameterFilter


@registry.register()
class ParameterNotFound(NotFound):
    pass


@registry.register()
class ParameterNotUnique(NotUnique):
    pass


@registry.register()
class ParameterDeletionPrevented(DeletionPrevented):
    pass


@registry.register()
class ParameterDataInvalid(OptimizationDataValidationError):
    pass


class ItemRepository(IndexedRepository[Parameter, ParameterIndexsetAssociation]):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    DataInvalid = ParameterDataInvalid

    target = db.r.ModelTarget(Parameter)
    association_target = db.r.ModelTarget(ParameterIndexsetAssociation)
    filter = db.r.Filter(ParameterFilter, Parameter)

    extra_data_columns = {"values", "units"}

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            ParameterIndexsetAssociation.parameter__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(db.r.ItemRepository[ParameterIndexsetAssociation]):
    target = db.r.ModelTarget(ParameterIndexsetAssociation)


class PandasRepository(db.r.PandasRepository):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = db.r.ModelTarget(Parameter)
    filter = db.r.Filter(ParameterFilter, Parameter)


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = db.r.ModelTarget(ParameterVersion)
    filter = db.r.Filter(ParameterFilter, ParameterVersion)
