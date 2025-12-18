from typing import Any, Sequence, overload

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.repository.base import BaseRepository, TargetT
from toolkit.db.repository.executor import AbstractExecutor
from toolkit.db.repository.filter import Filter
from toolkit.db.repository.target import DefaultModelT, ModelTarget

from ixmp4.data.model.db import Model
from ixmp4.data.run.db import Run


class AuthRepository(BaseRepository[TargetT]):
    auth_ctx: AuthorizationContext | None
    platform: PlatformProtocol | None
    target: ModelTarget[TargetT]

    def __init__(
        self,
        executor: AbstractExecutor,
        target: ModelTarget[TargetT] | None = None,
        filter: Filter | None = None,
        *,
        auth_ctx: AuthorizationContext | None = None,
        platform: PlatformProtocol | None = None,
    ):
        super().__init__(executor, target=target, filter=filter)

        self.auth_ctx = auth_ctx
        self.platform = platform

        if self.auth_ctx is not None and self.platform is not None:
            self.target = AuthModelTargetWrapper(
                self.target,
                auth_repo=self,
                auth_ctx=self.auth_ctx,
                platform=self.platform,
            )

    def select_permitted_model_ids(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> sa.Select[tuple[int]]:
        # managers (includes superusers) can access all models
        if auth_ctx.has_management_permission(platform):
            return sa.select(Model).with_only_columns(Model.id)

        perms = auth_ctx.tabulate_permissions(platform)
        if perms.is_empty():
            return sa.select(Model).where(sa.false()).with_only_columns(Model.id)

        like_conds = (
            Model.name.like(name_like) for name_like in perms["like"].to_list()
        )
        return sa.select(Model).where(sa.or_(*like_conds)).with_only_columns(Model.id)

    def select_permitted_run_ids(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> sa.Select[tuple[int]]:
        model_exc = self.select_permitted_model_ids(auth_ctx, platform)
        return (
            sa.select(Run).where(Run.model__id.in_(model_exc)).with_only_columns(Run.id)
        )

    @overload
    def where_authorized(
        self,
        exc: sa.Update,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Update: ...

    @overload
    def where_authorized(
        self,
        exc: sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Delete: ...

    @overload
    def where_authorized(
        self,
        exc: sa.Select[Any],
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any]: ...

    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        """
        Add WHERE clauses to the given SQLAlchemy statement to filter
        rows by permission criteria.

        Parameters
        ----------
        exc : sa.Select[SelectR] | sa.Update | sa.Delete
            The SQLAlchemy statement.
        auth_ctx : AuthorizationContext
            The `AuthorizationContext` object to use to assertain permissions.
        platform : PlatformProtocol
            The platform information object for the current platform.

        Returns
        -------
        sa.Select[SelectR] | sa.Update | sa.Delete
            The statement with permission WHERE clauses added.
        """
        raise NotImplementedError(
            f"`{self.__class__.__name__}` was instantiated with "
            "an `AuthorizationContext` but does not implement `where_authorized`."
        )


class AuthModelTargetWrapper(ModelTarget[DefaultModelT]):
    wrapped_target: ModelTarget[DefaultModelT]
    auth_repo: AuthRepository[DefaultModelT]
    auth_ctx: AuthorizationContext
    platform: PlatformProtocol

    def __init__(
        self,
        wrapped_target: ModelTarget[DefaultModelT],
        *,
        auth_repo: AuthRepository[DefaultModelT],
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ):
        self.wrapped_target = wrapped_target
        self.model_class = wrapped_target.model_class
        self.table = wrapped_target.table
        self.auth_repo = auth_repo
        self.auth_ctx = auth_ctx
        self.platform = platform

    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        exc = self.wrapped_target.select_statement(columns=columns)
        return self.auth_repo.where_authorized(exc, self.auth_ctx, self.platform)

    def insert_statement(self) -> sa.Insert:
        return self.wrapped_target.insert_statement()

    def update_statement(self) -> sa.Update:
        exc = self.wrapped_target.update_statement()
        return self.auth_repo.where_authorized(exc, self.auth_ctx, self.platform)

    def delete_statement(self) -> sa.Delete:
        exc = self.wrapped_target.delete_statement()
        return self.auth_repo.where_authorized(exc, self.auth_ctx, self.platform)

    def column(self, name: str) -> sa.ColumnElement[Any]:
        return self.wrapped_target.column(name)

    def pk_columns(self) -> sa.ColumnCollection[str, sa.ColumnElement[Any]]:
        return self.wrapped_target.pk_columns()

    def get_single_item(self, result: sa.Result[tuple[DefaultModelT]]) -> DefaultModelT:
        return self.wrapped_target.get_single_item(result)

    def get_item_list(
        self, result: sa.Result[tuple[DefaultModelT]]
    ) -> list[DefaultModelT]:
        return self.wrapped_target.get_item_list(result)
