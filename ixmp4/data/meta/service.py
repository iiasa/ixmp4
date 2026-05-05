from typing import List

from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.executor import SessionExecutor
from typing_extensions import Unpack

from ixmp4.base_exceptions import BadRequest, Forbidden
from ixmp4.data.compat_controller import EnumerationCompatibilityController
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.run.repositories import ItemRepository as RunItemRepository
from ixmp4.data.run.repositories import PandasRepository as RunPandasRepository
from ixmp4.data.services import DirectTransport, Http, Service, procedure

from .df_schemas import DeleteRunMetaFrameSchema, UpsertRunMetaFrameSchema
from .dto import MetaValueType, RunMetaEntry
from .filter import RunMetaEntryFilter
from .repositories import ItemRepository, PandasRepository


class RunMetaEntryService(Service):
    router_prefix = "/meta"
    router_tags = ["meta"]

    http_controller = EnumerationCompatibilityController
    executor: SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    runs: RunItemRepository
    runs_pandas: RunPandasRepository

    default_filter: RunMetaEntryFilter = {"run": {"default_only": True}}

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor, **self.get_auth_kwargs(transport))
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))
        self.runs = RunItemRepository(self.executor)
        self.runs_pandas = RunPandasRepository(self.executor)

    @procedure(Http(path="/", methods=("POST",)))
    def create(self, run_id: int, key: str, value: MetaValueType) -> RunMetaEntry:
        """Creates a metadata entry.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.run.dto.Run` for which this entry is
            defined.
        key : str
            The key (unique to this run) for which `value` is associated.
        value: MetaValueType
            The value for this entry.

        Raises
        ------
        :class:`RunMetaEntryNotUnique`:
            If the metadata entry with `key` and `run__id` is not unique.


        Returns
        -------
        :class:`RunMetaEntry`:
            The created metadata entry.
        """
        self.runs.get_by_pk({"id": run_id})
        self.items.create(run_id, key, value)
        return RunMetaEntry.model_validate(
            self.items.get({"run__id": run_id, "key": key})
        )

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        run_id: int,
        key: str,
        value: MetaValueType,
    ) -> None:
        run = self.runs.get_by_pk({"id": run_id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(methods=("POST",)))
    def get(self, run_id: int, key: str) -> RunMetaEntry:
        """Retrieves a metadata entry by the run id and key.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.run.dto.Run` for which this entry is
            defined.
        key : str
            The key (unique to this run) for which `value` is to be retrieved.

        Raises
        ------
        :class:`RunMetaEntryNotFound`:
            If the metadata entry with `id` does not exist.

        Returns
        -------
        :class:`RunMetaEntry`:
            The retrieved metadata entry.
        """
        return RunMetaEntry.model_validate(
            self.items.get({"run__id": run_id, "key": key})
        )

    @get.auth_check()
    def get_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        run_id: int,
        key: str,
    ) -> None:
        run = self.runs.get_by_pk({"id": run_id})
        auth_ctx.has_view_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(path="/{id:int}/", methods=("DELETE",)))
    def delete_by_id(self, id: int) -> None:
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
        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        id: int,
    ) -> None:
        entry = self.items.get_by_pk({"id": id})
        run = self.runs.get_by_pk({"id": entry.run__id})

        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[RunMetaEntryFilter]) -> list[RunMetaEntry]:
        r"""Lists metadata entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`RunMetaEntryFilter`.

        Returns
        -------
        Iterable[:class:`RunMetaEntry`]:
            List of metadata entries.
        """
        return [
            RunMetaEntry.model_validate(i)
            for i in self.items.list(values=self.apply_filter_defaults(kwargs))
        ]

    @list.auth_check()
    def list_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[RunMetaEntryFilter]
    ) -> PaginatedResult[List[RunMetaEntry]]:
        return PaginatedResult(
            results=[
                RunMetaEntry.model_validate(i)
                for i in self.items.list(
                    values=self.apply_filter_defaults(kwargs),
                    limit=pagination.limit,
                    offset=pagination.offset,
                )
            ],
            total=self.items.count(values=self.apply_filter_defaults(kwargs)),
            pagination=pagination,
        )

    def get_columns(
        self, *, join_run_index: bool | None, include_run_index: bool
    ) -> List[str]:
        if join_run_index is not None:
            include_run_index = join_run_index

        columns = [
            "id",
            "dtype",
            "key",
            "value_int",
            "value_str",
            "value_float",
            "value_bool",
        ]

        if include_run_index:
            columns += ["model", "scenario", "version"]
        else:
            columns += ["run__id"]

        return columns

    @procedure(Http(methods=("PATCH",)))
    def tabulate(
        self,
        include_run_index: bool = False,
        join_run_index: bool | None = None,
        **kwargs: Unpack[RunMetaEntryFilter],
    ) -> SerializableDataFrame:
        r"""Tabulates metadata entries by specified criteria.

        Parameters
        ----------
        include_run_index: bool, optional
            Whether to include run columns in the data frame.
            Default: ``False``
        \*\*kwargs: any
            Filter parameters as specified in :class:`RunMetaEntryFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - dtype
                - key
                - value
            if ``include_run_index`` is ``False`` (default):
                - run__id
            if ``include_run_index`` is ``True``:
                - model
                - scenario
                - version

        """

        return self.pandas.tabulate(
            values=self.apply_filter_defaults(kwargs),
            columns=self.get_columns(
                join_run_index=join_run_index, include_run_index=include_run_index
            ),
        )

    @tabulate.auth_check()
    def tabulate_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self,
        pagination: Pagination,
        include_run_index: bool = False,
        join_run_index: bool | None = None,
        **kwargs: Unpack[RunMetaEntryFilter],
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult[SerializableDataFrame](
            results=self.pandas.tabulate(
                values=self.apply_filter_defaults(kwargs),
                limit=pagination.limit,
                offset=pagination.offset,
                columns=self.get_columns(
                    join_run_index=join_run_index, include_run_index=include_run_index
                ),
            ),
            total=self.pandas.count(values=self.apply_filter_defaults(kwargs)),
            pagination=pagination,
        )

    @procedure(Http(methods=("POST",)))
    def bulk_upsert(self, df: SerializableDataFrame) -> None:
        """Upserts a dataframe of run meta indicator entries.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - run__id
                - key
                - value
        """
        df = self.validate_df_or_raise(df, UpsertRunMetaFrameSchema)
        self.pandas.upsert(df)

    @bulk_upsert.auth_check()
    def bulk_upsert_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        df: SerializableDataFrame,
    ) -> None:
        if "run__id" not in df.columns:
            raise BadRequest("Column 'run__id' is required for bulk upsert auth check.")

        run_models = (
            self.runs_pandas.tabulate(
                {"id__in": df["run__id"].tolist()}, columns=["id", "model"]
            )["model"]
            .unique()
            .tolist()
        )
        auth_ctx.has_edit_permission(platform, models=run_models, raise_exc=Forbidden)

    @procedure(Http(methods=("DELETE",)))
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
        df = self.validate_df_or_raise(df, DeleteRunMetaFrameSchema)
        self.pandas.delete(df)

    @bulk_delete.auth_check()
    def bulk_delete_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        df: SerializableDataFrame,
    ) -> None:
        if "run__id" not in df.columns:
            raise BadRequest("Column 'run__id' is required for bulk delete auth check.")

        run_models = (
            self.runs_pandas.tabulate(
                {"id__in": df["run__id"].tolist()}, columns=["id", "model"]
            )["model"]
            .unique()
            .tolist()
        )
        auth_ctx.has_edit_permission(platform, models=run_models, raise_exc=Forbidden)
