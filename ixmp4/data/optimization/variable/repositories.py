from typing import Any, Generic, TypeVar

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Variable, VariableIndexsetAssociation, VariableVersion
from .exceptions import (
    VariableDataInvalid,
    VariableNotFound,
    VariableNotUnique,
)
from .filter import VariableFilter, VariableVersionFilter

VariableTargetT = TypeVar("VariableTargetT")


class VariableAuthRepository(
    AuthRepository[VariableTargetT],
    Generic[VariableTargetT],
):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(Variable.run__id.in_(run_exc))


class VariableVersionAuthRepository(AuthRepository[VariableVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(VariableVersion.run__id.in_(run_exc))


class ItemRepository(
    VariableAuthRepository[Variable],
    IndexedRepository[Variable, VariableIndexsetAssociation],
):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    DataInvalid = VariableDataInvalid

    target = ModelTarget(Variable)
    association_target = ModelTarget(VariableIndexsetAssociation)
    filter = Filter(VariableFilter, Variable)

    extra_data_columns = {"levels", "marginals"}

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            VariableIndexsetAssociation.variable__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(BaseItemRepository[VariableIndexsetAssociation]):
    target = ModelTarget(VariableIndexsetAssociation)


class PandasRepository(VariableAuthRepository[Variable], BasePandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = ModelTarget(Variable)
    filter = Filter(VariableFilter, Variable)


class VersionRepository(VariableVersionAuthRepository, BasePandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = ModelTarget(VariableVersion)
    filter = Filter(VariableVersionFilter, VariableVersion)
