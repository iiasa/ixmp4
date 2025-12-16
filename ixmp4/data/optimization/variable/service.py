from typing import Any, List

import pandas as pd
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden, OptimizationItemUsageError
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.optimization.base.service import IndexSetAssociatedService
from ixmp4.data.optimization.indexset.repositories import (
    ItemRepository as IndexSetRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import DirectTransport, Http, procedure

from .db import VariableDocs
from .dto import Variable
from .filter import VariableFilter
from .repositories import (
    AssociationRepository,
    ItemRepository,
    PandasRepository,
    VariableDataInvalid,
    VersionRepository,
)


class VariableService(DocsService, IndexSetAssociatedService):
    router_prefix = "/optimization/variables"
    router_tags = ["optimization", "variables"]

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
        DocsService.__init_direct__(self, transport, docs_model=VariableDocs)

    @procedure(Http(path="/", methods=("POST",)))
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
        """Creates a table.

        Variables
        ----------
        TODO


        Raises
        ------
        :class:`VariableNotUnique`:
            If the table is not unique.
        :class:`OptimizationItemUsageError`:
            If the table arguments are not valid.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Variable`:
            The created table.
        """

        self.check_optional_column_args(
            name, "Variable", constrained_to_indexsets, column_names
        )

        self.items.create({"name": name, "run__id": run_id, **self.get_creation_info()})
        db_equ = self.items.get({"name": name, "run__id": run_id})

        if constrained_to_indexsets:
            nullable_column_names: list[str] | list[None] = column_names or (
                [None] * len(constrained_to_indexsets)
            )

            for idxset_name, col_name in zip(
                constrained_to_indexsets, nullable_column_names
            ):
                indexset = self.indexsets.get({"name": idxset_name, "run__id": run_id})
                self.associations.create(
                    {
                        "variable__id": db_equ.id,
                        "indexset__id": indexset.id,
                        "column_name": col_name,
                    }
                )

        return Variable.model_validate(db_equ)

    @create.auth_check()
    def create_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        # TODO: Check run_id
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=("POST",)))
    def get(self, run_id: int, name: str) -> Variable:
        """Retrieves a table by its name and run_id.

        Variables
        ----------
        run_id : int
            The unique name of the table.

        name : str
            The unique name of the table.

        Raises
        ------
        :class:`VariableNotFound`:
            If the table with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Variable`:
            The retrieved table.
        """
        return Variable.model_validate(
            self.items.get({"name": name, "run__id": run_id})
        )

    @get.auth_check()
    def get_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/", methods=("GET",)))
    def get_by_id(self, id: int) -> Variable:
        """Retrieves a table by its id.

        Variables
        ----------
        id : int
            The integer id of the table.

        Raises
        ------
        :class:`VariableNotFound`:
            If the table with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Variable`:
            The retrieved table.
        """

        return Variable.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/", methods=("DELETE",)))
    def delete_by_id(self, id: int) -> None:
        """Deletes a table.

        Variables
        ----------
        id : int
            The unique integer id of the table.

        Raises
        ------
        :class:`VariableNotFound`:
            If the table with `id` does not exist.
        :class:`VariableDeletionPrevented`:
            If the table with `id` is used in the database, preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """
        self.items.delete_associations(id)
        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_by_id_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/data", methods=("POST",)))
    def add_data(self, id: int, data: dict[str, Any] | SerializableDataFrame) -> None:
        """Adds data to a Variable.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Variable's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Variable.data`, existing values will be
        overwritten.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Variable`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        :class:`ixmp4.core.exceptions.OptimizationItemUsageError`:
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
                raise VariableDataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                "Variable.data must include the column(s): "
                f"{', '.join(sorted(missing_columns))}!"
            )

        self.items.add_data(id, data)

    @add_data.auth_check()
    def add_data_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/data", methods=("DELETE",)))
    def remove_data(
        self, id: int, data: dict[str, Any] | SerializableDataFrame | None = None
    ) -> None:
        """Removes data from a Variable.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Variable`.
        data : dict[str, Any] | pandas.DataFrame, optional
            The data to be removed. If specified, remove only specific data. This must
            specify all indexed columns. All other keys/columns are ignored. Otherwise,
            remove all data (the default).

        Returns
        -------
        None
        """

        if data is None:
            # Remove all data per default
            # TODO Is there a better way to reset .data?
            self.items.update_by_pk({"id": id, "data": {}})
        else:
            if isinstance(data, dict):
                data = pd.DataFrame.from_dict(data=data)

            if data.empty:
                return

            self.items.remove_data(id, data)

    @remove_data.auth_check()
    def remove_data_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[VariableFilter]) -> list[Variable]:
        r"""Lists variables by specified criteria.

        Variables
        ----------
        \*\*kwargs: any
            Filter variables as specified in `VariableFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Variable`]:
            List of variables.
        """
        return [Variable.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.auth_check()
    def list_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        **kwargs: Unpack[VariableFilter],
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[VariableFilter]
    ) -> PaginatedResult[List[Variable]]:
        return PaginatedResult(
            results=[
                Variable.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=("PATCH",)))
    def tabulate(self, **kwargs: Unpack[VariableFilter]) -> SerializableDataFrame:
        r"""Tabulates variables by specified criteria.

        Variables
        ----------
        \*\*kwargs: any
            Filter variables as specified in `VariableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """

        return self.pandas.tabulate(values=kwargs)

    @tabulate.auth_check()
    def tabulate_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[VariableFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult[SerializableDataFrame](
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
