from toolkit import db

from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Parameter, ParameterIndexsetAssociation, ParameterVersion
from .exceptions import ParameterDataInvalid, ParameterNotFound, ParameterNotUnique
from .filter import ParameterFilter


class ItemRepository(IndexedRepository[ParameterIndexsetAssociation]):
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


class VersionRepository(db.r.PandasRepository):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = db.r.ModelTarget(ParameterVersion)
    filter = db.r.Filter(ParameterFilter, ParameterVersion)
