from typing import Any, Generic, TypeVar

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.optimization.base.repositories import IndexedRepository

from .db import Equation, EquationIndexsetAssociation, EquationVersion
from .exceptions import EquationDataInvalid, EquationNotFound, EquationNotUnique
from .filter import EquationFilter

EquationTargetT = TypeVar("EquationTargetT")


class EquationAuthRepository(AuthRepository[EquationTargetT], Generic[EquationTargetT]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(Equation.run__id.in_(run_exc))


class ItemRepository(
    EquationAuthRepository[Equation],
    IndexedRepository[Equation, EquationIndexsetAssociation],
):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    DataInvalid = EquationDataInvalid
    target = ModelTarget(Equation)
    association_target = ModelTarget(EquationIndexsetAssociation)
    filter = Filter(EquationFilter, Equation)
    extra_data_columns = {"levels", "marginals"}

    def delete_associations(self, id: int) -> None | int:
        exc = self.association_target.delete_statement().where(
            EquationIndexsetAssociation.equation__id == id
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as rowcount:
                return rowcount


class AssociationRepository(BaseItemRepository[EquationIndexsetAssociation]):
    target = ModelTarget(EquationIndexsetAssociation)


class PandasRepository(EquationAuthRepository[Equation], BasePandasRepository):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    target = ModelTarget(Equation)
    filter = Filter(EquationFilter, Equation)


class VersionRepository(EquationAuthRepository[EquationVersion], BasePandasRepository):
    NotFound = EquationNotFound
    NotUnique = EquationNotUnique
    target = ModelTarget(EquationVersion)
    filter = Filter(EquationFilter, EquationVersion)
