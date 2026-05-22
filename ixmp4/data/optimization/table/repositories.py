from typing import Any, Generic, TypeVar

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Table, TableIndexsetAssociation, TableVersion
from .exceptions import (
    TableDataInvalid,
    TableNotFound,
    TableNotUnique,
)
from .filter import TableFilter, TableVersionFilter

TableTargetT = TypeVar("TableTargetT")


class TableAuthRepository(AuthRepository[TableTargetT], Generic[TableTargetT]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(Table.run__id.in_(run_exc))


class TableVersionAuthRepository(AuthRepository[TableVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(TableVersion.run__id.in_(run_exc))


class ItemRepository(
    TableAuthRepository[Table], IndexedRepository[Table, TableIndexsetAssociation]
):
    NotFound = TableNotFound
    NotUnique = TableNotUnique
    DataInvalid = TableDataInvalid
    target = ModelTarget(Table)
    association_target = ModelTarget(TableIndexsetAssociation)
    filter = Filter(TableFilter, Table)

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            TableIndexsetAssociation.table__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(BaseItemRepository[TableIndexsetAssociation]):
    target = ModelTarget(TableIndexsetAssociation)


class PandasRepository(TableAuthRepository[Table], BasePandasRepository):
    NotFound = TableNotFound
    NotUnique = TableNotUnique
    target = ModelTarget(Table)
    filter = Filter(TableFilter, Table)


class VersionRepository(TableVersionAuthRepository, BasePandasRepository):
    NotFound = TableNotFound
    NotUnique = TableNotUnique
    target = ModelTarget(TableVersion)
    filter = Filter(TableVersionFilter, TableVersion)
