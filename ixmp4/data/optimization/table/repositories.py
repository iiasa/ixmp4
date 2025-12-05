from toolkit import db

from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Table, TableIndexsetAssociation, TableVersion
from .exceptions import TableDataInvalid, TableNotFound, TableNotUnique
from .filter import TableFilter


class ItemRepository(IndexedRepository[Table, TableIndexsetAssociation]):
    NotFound = TableNotFound
    NotUnique = TableNotUnique
    DataInvalid = TableDataInvalid
    target = db.r.ModelTarget(Table)
    association_target = db.r.ModelTarget(TableIndexsetAssociation)
    filter = db.r.Filter(TableFilter, Table)

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            TableIndexsetAssociation.table__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(db.r.ItemRepository[TableIndexsetAssociation]):
    target = db.r.ModelTarget(TableIndexsetAssociation)


class PandasRepository(db.r.PandasRepository):
    NotFound = TableNotFound
    NotUnique = TableNotUnique
    target = db.r.ModelTarget(Table)
    filter = db.r.Filter(TableFilter, Table)


class VersionRepository(db.r.PandasRepository):
    NotFound = TableNotFound
    NotUnique = TableNotUnique
    target = db.r.ModelTarget(TableVersion)
    filter = db.r.Filter(TableFilter, TableVersion)
