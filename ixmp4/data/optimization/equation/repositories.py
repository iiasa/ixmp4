from typing import Any

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Equation, EquationIndexsetAssociation, EquationVersion
from .exceptions import EquationDataInvalid, EquationNotFound, EquationNotUnique
from .filter import EquationFilter


class EquationAuthRepository(AuthRepository[Equation]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(Equation.run__id.in_(run_exc))


class ItemRepository(
    EquationAuthRepository, IndexedRepository[Equation, EquationIndexsetAssociation]
):
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


class PandasRepository(EquationAuthRepository, db.r.PandasRepository):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    target = db.r.ModelTarget(Equation)
    filter = db.r.Filter(EquationFilter, Equation)


class VersionRepository(db.r.PandasRepository):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    target = db.r.ModelTarget(EquationVersion)
    filter = db.r.Filter(EquationFilter, EquationVersion)
