from typing import Any, List, cast

from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.optimization.equation.repositories import (
    ItemRepository as EquationRepository,
)
from ixmp4.data.optimization.parameter.repositories import (
    ItemRepository as ParameterRepository,
)
from ixmp4.data.optimization.table.repositories import (
    ItemRepository as TableRepository,
)
from ixmp4.data.optimization.variable.repositories import (
    ItemRepository as VariableRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import DirectTransport, GetByIdService, Http, procedure

from .db import IndexSetDocs
from .dto import IndexSet
from .filter import IndexSetFilter
from .repositories import (
    IndexSetDataItemRepository,
    ItemRepository,
    PandasRepository,
    VersionRepository,
)


class IndexSetService(DocsService, GetByIdService):
    router_prefix = "/optimization/indexsets"
    router_tags = ["optimization", "indexsets"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    data: IndexSetDataItemRepository
    pandas: PandasRepository
    versions: VersionRepository

    equations: EquationRepository
    parameters: ParameterRepository
    tables: TableRepository
    variables: VariableRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.data = IndexSetDataItemRepository(self.executor)
        self.versions = VersionRepository(self.executor)
        self.equations = EquationRepository(self.executor)
        self.parameters = ParameterRepository(self.executor)
        self.tables = TableRepository(self.executor)
        self.variables = VariableRepository(self.executor)

        DocsService.__init_direct__(self, transport, docs_model=IndexSetDocs)

    @procedure(Http(path="/", methods=["POST"]))
    def create(
        self,
        run_id: int,
        name: str,
    ) -> IndexSet:
        """Creates an indexset.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this IndexSet is
            defined.
        name : str
            The name of the IndexSet.

        Raises
        ------
        :class:`IndexSetNotUnique`:
            If the indexset is not unique.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`IndexSet`:
            The created indexset.
        """

        self.items.create(
            {
                "name": name,
                "run__id": run_id,
                "data_type": None,
                **self.get_creation_info(),
            }
        )
        return IndexSet.model_validate(
            self.items.get({"name": name, "run__id": run_id})
        )

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

    @procedure(Http(methods=["POST"]))
    def get(self, run_id: int, name: str) -> IndexSet:
        """Retrieves an indexset by its name and run_id.

        Parameters
        ----------
        run_id : int
            The unique name of the indexset.

        name : str
            The unique name of the indexset.

        Raises
        ------
        :class:`IndexSetNotFound`:
            If the indexset with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`IndexSet`:
            The retrieved indexset.
        """
        return IndexSet.model_validate(
            self.items.get({"name": name, "run__id": run_id})
        )

    @get.auth_check()
    def get_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id}/", methods=["GET"]))
    def get_by_id(self, id: int) -> IndexSet:
        """Retrieves an indexset by its id.

        Parameters
        ----------
        id : int
            The integer id of the indexset.

        Raises
        ------
        :class:`IndexSetNotFound`:
            If the indexset with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`IndexSet`:
            The retrieved indexset.
        """

        return IndexSet.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id}/", methods=["DELETE"]))
    def delete_by_id(self, id: int) -> None:
        """Deletes an indexset.

        Parameters
        ----------
        id : int
            The unique integer id of the indexset.

        Raises
        ------
        :class:`IndexSetNotFound`:
            If the indexset with `id` does not exist.
        :class:`IndexSetDeletionPrevented`:
            If the indexset with `id` is used in the database, preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """

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

    def data_to_str_list(self, data: List[float] | List[int] | List[str]) -> List[str]:
        return [str(d) for d in data]

    def data_to_data_list(
        self, data: float | int | str | List[float] | List[int] | List[str]
    ) -> List[float] | List[int] | List[str]:
        if isinstance(data, list):
            return data
        else:
            return cast(List[float] | List[int] | List[str], [data])

    @procedure(Http(path="/{id}/data", methods=["POST"]))
    def add_data(
        self, id: int, data: float | int | str | List[float] | List[int] | List[str]
    ) -> None:
        """Adds data to an existing IndexSet.

        Parameters
        ----------
        id : int
            The id of the target IndexSet.
        data : float | int | str | List[float] | List[int] | List[str]
            The data to be added to the IndexSet.

        Returns
        -------
        None
        """
        data_list = self.data_to_data_list(data)

        if len(data_list) == 0:
            return  # nothing to be done

        data_type = self.items.check_type(id, data_list)
        self.data.add(id, self.data_to_str_list(data_list))
        self.items.update_by_pk({"id": id, "data_type": data_type})

    @add_data.auth_check()
    def add_data_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id}/data", methods=["DELETE"]))
    def remove_data(
        self,
        id: int,
        data: float | int | str | List[float] | List[int] | List[str],
        remove_dependent_data: bool = True,
    ) -> None:
        """Removes data from an existing IndexSet.

        Parameters
        ----------
        id : int
            The id of the target IndexSet.
        data : float | int | str | List[float] | List[int] | List[str]
            The data to be removed from the IndexSet.
        remove_dependent_data : bool, optional
            Whether to delete data from all linked items referencing `data`.
            Default: `True`.

        Returns
        -------
        None
        """
        data_list = self.data_to_data_list(data)

        # NOTE Should remove_dependent_data be removed, changed, see https://github.com/iiasa/ixmp4/issues/136
        if not bool(data_list):
            return

        self.items.check_type(id, data_list)

        str_list = self.data_to_str_list(data_list)
        if remove_dependent_data:
            indexset = self.items.get_by_pk({"id": id})
            self.equations.remove_invalid_linked_data(indexset, data_list)
            self.parameters.remove_invalid_linked_data(indexset, data_list)
            self.tables.remove_invalid_linked_data(indexset, data_list)
            self.variables.remove_invalid_linked_data(indexset, data_list)

        self.data.remove(id, str_list)
        self.items.reset_type(id)

    @remove_data.auth_check()
    def remove_data_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=["PATCH"]))
    def list(self, **kwargs: Unpack[IndexSetFilter]) -> list[IndexSet]:
        r"""Lists indexsets by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `IndexSetFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`IndexSet`]:
            List of indexsets.
        """
        return [IndexSet.model_validate(i) for i in self.items.list(values=kwargs)]

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
        self, pagination: Pagination, **kwargs: Unpack[IndexSetFilter]
    ) -> PaginatedResult[List[IndexSet]]:
        return PaginatedResult(
            results=[
                IndexSet.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=["PATCH"]))
    def tabulate(self, **kwargs: Unpack[IndexSetFilter]) -> SerializableDataFrame:
        r"""Tabulates indexsets by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `IndexSet\Filter`.

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
        self, pagination: Pagination, **kwargs: Unpack[IndexSetFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
