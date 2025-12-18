from typing import Any

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Parameter, ParameterIndexsetAssociation, ParameterVersion
from .exceptions import ParameterDataInvalid, ParameterNotFound, ParameterNotUnique
from .filter import ParameterFilter


class ParameterAuthRepository(AuthRepository[Parameter]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Parameter.run__id.in_(run_exc))


class ItemRepository(
    ParameterAuthRepository, IndexedRepository[Parameter, ParameterIndexsetAssociation]
):
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


class PandasRepository(ParameterAuthRepository, db.r.PandasRepository):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = db.r.ModelTarget(Parameter)
    filter = db.r.Filter(ParameterFilter, Parameter)


class VersionRepository(db.r.PandasRepository):
    NotFound = ParameterNotFound
    NotUnique = ParameterNotUnique
    target = db.r.ModelTarget(ParameterVersion)
    filter = db.r.Filter(ParameterFilter, ParameterVersion)
