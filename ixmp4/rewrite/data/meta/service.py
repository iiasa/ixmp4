from typing import List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.data.run.repositories import ItemRepository as RunRepository
from ixmp4.rewrite.exceptions import Forbidden
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .dto import MetaValueType, RunMetaEntry
from .filter import RunMetaEntryFilter
from .repositories import ItemRepository, PandasRepository


class RunMetaEntryService(Service):
    router_prefix = "/meta"
    router_tags = ["meta"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    runs: RunRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.runs = RunRepository(self.executor)

    @procedure(methods=["POST"])
    def create(self, run__id: int, key: str, value: MetaValueType) -> RunMetaEntry:
        """Creates a metadata entry.

        Parameters
        ----------
        TODO

        Raises
        ------
        :class:`RunMetaEntryNotUnique`:
            If the metadata entry with `name` is not unique.


        Returns
        -------
        :class:`RunMetaEntry`:
            The created metadata entry.
        """
        run = self.runs.get_by_pk({"id": run__id})

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(
                self.platform, models=[run.model.name], raise_exc=Forbidden
            )

        self.items.create(run__id, key, value)
        return RunMetaEntry.model_validate(
            self.items.get({"run__id": run__id, "key": key})
        )

    @procedure(methods=["POST"])
    def get(self, run__id: int, key: str) -> RunMetaEntry:
        """Retrieves a metadata entry by the run id and key.

        Parameters
        ----------
        TODO

        Raises
        ------
        :class:`RunMetaEntryNotFound`:
            If the metadata entry with `id` does not exist.

        Returns
        -------
        :class:`RunMetaEntry`:
            The retrieved metadata entry.
        """
        run = self.runs.get_by_pk({"id": run__id})

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(
                self.platform, models=[run.model.name], raise_exc=Forbidden
            )

        return RunMetaEntry.model_validate(
            self.items.get({"run__id": run__id, "key": key})
        )

    @procedure(path="/{id}/", methods=["DELETE"])
    def delete(self, id: int) -> None:
        """Deletes a metadata entry.

        Parameters
        ----------
        id: int
            Unique integer id of the entry to delete

        Raises
        ------
        :class:`RunMetaEntryNotFound`:
            If the metadata entry with `id` was not found.

        """
        entry = self.items.get_by_pk({"id": id})
        run = self.runs.get_by_pk({"id": entry.run__id})

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(
                self.platform, models=[run.model.name], raise_exc=Forbidden
            )

        self.items.delete_by_pk({"id": id})

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[RunMetaEntryFilter]) -> list[RunMetaEntry]:
        r"""Lists metadata entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `RunMetaEntryFilter`.

        Returns
        -------
        Iterable[:class:`RunMetaEntry`]:
            List of metadata entries.
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return [RunMetaEntry.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[RunMetaEntryFilter]
    ) -> PaginatedResult[List[RunMetaEntry]]:
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return PaginatedResult(
            results=[
                RunMetaEntry.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    def get_requested_columns(self, join_run_index: bool) -> List[str]:
        columns = [
            "id",
            "dtype",
            "key",
            "value_int",
            "value_str",
            "value_float",
            "value_bool",
        ]

        if join_run_index:
            columns += ["model", "scenario", "version"]
        else:
            columns += ["run__id"]

        return columns

    @paginated_procedure(methods=["PATCH"])
    def tabulate(
        self, join_run_index: bool = False, **kwargs: Unpack[RunMetaEntryFilter]
    ) -> SerializableDataFrame:
        r"""Tabulates metadata entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `RunMetaEntryFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - TODO
        """

        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return self.pandas.tabulate(
            values=kwargs, columns=self.get_requested_columns(join_run_index)
        )

    @tabulate.paginated()
    def paginated_tabulate(
        self,
        pagination: Pagination,
        join_run_index: bool = False,
        **kwargs: Unpack[RunMetaEntryFilter],
    ) -> PaginatedResult[SerializableDataFrame]:
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs,
                limit=pagination.limit,
                offset=pagination.offset,
                columns=self.get_requested_columns(join_run_index),
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(methods=["POST"])
    def bulk_upsert(self, df: SerializableDataFrame) -> None:
        """Upserts a dataframe of run meta indicator entries.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - run__id
                - key
                - value
                - type
        """

        # TODO check run__ids
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

        self.pandas.upsert(df)

    @procedure(methods=["DELETE"])
    def bulk_delete(self, df: SerializableDataFrame) -> None:
        """Deletes run meta indicator entries as specified per dataframe.
        Warning: No recovery of deleted data shall be possible via ixmp
        after the execution of this function.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - run__id
                - key

        """

        # TODO check run__ids
        @self.auth_check
        def auth_check(auth_ctx: AuthorizationContext, platform: Ixmp4Instance):
            auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

        self.pandas.delete(df)
