from typing import Any, Generic, TypeVar

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Parameter, ParameterIndexsetAssociation, ParameterVersion
from .exceptions import ParameterDataInvalid, ParameterNotFound, ParameterNotUnique
from .filter import ParameterFilter, ParameterVersionFilter

ParameterTargetT = TypeVar("ParameterTargetT")


class ParameterAuthRepository(
    AuthRepository[ParameterTargetT],
    Generic[ParameterTargetT],
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
        return exc.where(Parameter.run__id.in_(run_exc))


class ParameterVersionAuthRepository(AuthRepository[ParameterVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(ParameterVersion.run__id.in_(run_exc))


class ItemRepository(
    ParameterAuthRepository[Parameter],
    IndexedRepository[Parameter, ParameterIndexsetAssociation],
):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    DataInvalid = ParameterDataInvalid

    target = ModelTarget(Parameter)
    association_target = ModelTarget(ParameterIndexsetAssociation)
    filter = Filter(ParameterFilter, Parameter)

    extra_data_columns = {"values", "units"}

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            ParameterIndexsetAssociation.parameter__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(BaseItemRepository[ParameterIndexsetAssociation]):
    target = ModelTarget(ParameterIndexsetAssociation)


class PandasRepository(ParameterAuthRepository[Parameter], BasePandasRepository):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = ModelTarget(Parameter)
    filter = Filter(ParameterFilter, Parameter)


class VersionRepository(
    ParameterVersionAuthRepository,
    BasePandasRepository,
):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = ModelTarget(ParameterVersion)
    filter = Filter(ParameterVersionFilter, ParameterVersion)
