import logging
from typing import Any, List

import pandas as pd
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.optimization.base.service import IndexSetAssociatedService
from ixmp4.data.optimization.indexset.repositories import (
    ItemRepository as IndexSetRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import DirectTransport, Http, procedure

from .db import TableDocs
from .dto import Table
from .filter import TableFilter
from .repositories import (
    AssociationRepository,
    ItemRepository,
    PandasRepository,
    TableDataInvalid,
    VersionRepository,
)

logger = logging.getLogger(__name__)


class TableService(DocsService, IndexSetAssociatedService):
    router_prefix = "/optimization/tables"
    router_tags = ["optimization", "tables"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    versions: VersionRepository

    associations: AssociationRepository
    indexsets: IndexSetRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.versions = VersionRepository(self.executor)
        self.associations = AssociationRepository(self.executor)
        self.indexsets = IndexSetRepository(self.executor)
        DocsService.__init_direct__(self, transport, docs_model=TableDocs)

    @procedure(Http(path="/", methods=("POST",)))
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Table:
        """Creates a table.

        Each column of the Table needs to be constrained to an existing
        :class:`ixmp4.data.abstract.optimization.IndexSet`. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.


        Parameters
        ----------
        ----------
        run_id : int
            The id of the :class:`Run` for which this Table is
            defined.
        name : str
            The unique name of the Table.
        constrained_to_indexsets : list[str]
            List of :class:`IndexSet` names that define
            the allowed contents of the Table's columns.
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.


        Raises
        ------
        :class:`TableNotUnique`:
            If the table is not unique.
        :class:`OptimizationItemUsageError`:
            If the table arguments are not valid.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Table`:
            The created table.
        """

        nullable_column_names: list[str] | list[None]

        if column_names:
            self.check_column_args(
                name, "Table", constrained_to_indexsets, column_names
            )
            nullable_column_names = column_names
        else:
            nullable_column_names = [None] * len(constrained_to_indexsets)

        self.items.create({"name": name, "run__id": run_id, **self.get_creation_info()})
        db_tab = self.items.get({"name": name, "run__id": run_id})

        if constrained_to_indexsets:
            for idxset_name, col_name in zip(
                constrained_to_indexsets, nullable_column_names
            ):
                indexset = self.indexsets.get({"name": idxset_name, "run__id": run_id})
                self.associations.create(
                    {
                        "table__id": db_tab.id,
                        "indexset__id": indexset.id,
                        "column_name": col_name,
                    }
                )

        return Table.model_validate(db_tab)

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # TODO: Check run_id
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=("POST",)))
    def get(self, run_id: int, name: str) -> Table:
        """Retrieves a table by its name and run_id.

        Parameters
        ----------
        run_id : int
            The unique name of the table.

        name : str
            The unique name of the table.

        Raises
        ------
        :class:`TableNotFound`:
            If the table with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Table`:
            The retrieved table.
        """
        return Table.model_validate(self.items.get({"name": name, "run__id": run_id}))

    @get.auth_check()
    def get_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/", methods=("GET",)))
    def get_by_id(self, id: int) -> Table:
        """Retrieves a table by its id.

        Parameters
        ----------
        id : int
            The integer id of the table.

        Raises
        ------
        :class:`TableNotFound`:
            If the table with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Table`:
            The retrieved table.
        """

        return Table.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/", methods=("DELETE",)))
    def delete_by_id(self, id: int) -> None:
        """Deletes a table.

        Parameters
        ----------
        id : int
            The unique integer id of the table.

        Raises
        ------
        :class:`TableNotFound`:
            If the table with `id` does not exist.
        :class:`TableDeletionPrevented`:
            If the table with `id` is used in the database, preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """

        self.items.delete_associations(id)
        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/data", methods=("POST",)))
    def add_data(self, id: int, data: dict[str, Any] | SerializableDataFrame) -> None:
        """Adds data to a Table.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Table's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Table.data`, existing values will be
        overwritten.

        Parameters
        ----------
        id : int
            The id of the :class:`Table`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        :class:`OptimizationDataValidationError`:
            - If values are missing, `None`, or `NaN`
            - If values are not allowed based on constraints to `Indexset`s
            - If rows are not unique

        Returns
        -------
        None
        """

        if isinstance(data, dict):
            try:
                data = pd.DataFrame.from_dict(data=data)
            except ValueError as e:
                raise TableDataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        self.items.add_data(id, data)

    @add_data.auth_check()
    def add_data_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/data", methods=("DELETE",)))
    def remove_data(
        self, id: int, data: dict[str, Any] | SerializableDataFrame | None = None
    ) -> None:
        """Removes data from a Table.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Table`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be removed. This must specify all indexed columns. All other
            keys/columns are ignored.

        Returns
        -------
        None
        """

        if data is None:
            self.items.update_by_pk({"id": id, "data": {}})
        else:
            if isinstance(data, dict):
                data = pd.DataFrame.from_dict(data=data)

            if data.empty:
                return

            self.items.remove_data(id, data)

    @remove_data.auth_check()
    def remove_data_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[TableFilter]) -> list[Table]:
        r"""Lists tables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter tables as specified in `TableFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Table`]:
            List of tables.
        """
        return [Table.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.auth_check()
    def list_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[TableFilter]
    ) -> PaginatedResult[List[Table]]:
        return PaginatedResult(
            results=[
                Table.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=("PATCH",)))
    def tabulate(self, **kwargs: Unpack[TableFilter]) -> SerializableDataFrame:
        r"""Tabulates tables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter tables as specified in `TableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - data
                - run__id
                - created_at
                - created_by
        """

        return self.pandas.tabulate(values=kwargs)

    @tabulate.auth_check()
    def tabulate_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[TableFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult[SerializableDataFrame](
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
