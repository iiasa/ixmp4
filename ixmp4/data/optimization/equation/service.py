import logging
from typing import Any, List

import pandas as pd
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.manager.models import Ixmp4Instance
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

from .db import EquationDocs
from .dto import Equation
from .filter import EquationFilter
from .repositories import (
    AssociationRepository,
    EquationDataInvalid,
    ItemRepository,
    PandasRepository,
    VersionRepository,
)

logger = logging.getLogger(__name__)


class EquationService(DocsService, IndexSetAssociatedService):
    router_prefix = "/optimization/equations"
    router_tags = ["optimization", "equations"]

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
        DocsService.__init_direct__(self, transport, docs_model=EquationDocs)

    @procedure(Http(path="/", methods=["POST"]))
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Equation:
        """Creates a equation.

        Each column of the Equation needs to be constrained to an existing
        :class:`ixmp4.data.abstract.optimization.IndexSet`. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Equation is
            defined.
        name : str
            The unique name of the Equation.
        constrained_to_indexsets : list[str] | None = None
            List of :class:`ixmp4.data.abstract.optimization.IndexSet` names that define
            the allowed contents of the Equation's columns. If None, no data
            can be added beyond `levels` and `marginals`!
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.

        Raises
        ------
        :class:`EquationNotUnique`:
            If the equation is not unique.
        :class:`OptimizationItemUsageError`:
            If the equation arguments are not valid.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Equation`:
            The created equation.
        """
        self.check_optional_column_args(
            name, "Equation", constrained_to_indexsets, column_names
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
                        "equation__id": db_equ.id,
                        "indexset__id": indexset.id,
                        "column_name": col_name,
                    }
                )

        return Equation.model_validate(db_equ)

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        # TODO: Check run_id
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(methods=["POST"]))
    def get(self, run_id: int, name: str) -> Equation:
        """Retrieves a equation by its name and run_id.

        Parameters
        ----------
        run_id : int
            The unique name of the equation.

        name : str
            The unique name of the equation.

        Raises
        ------
        :class:`EquationNotFound`:
            If the equation with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Equation`:
            The retrieved equation.
        """
        return Equation.model_validate(
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
    def get_by_id(self, id: int) -> Equation:
        """Retrieves a equation by its id.

        Parameters
        ----------
        id : int
            The integer id of the equation.

        Raises
        ------
        :class:`EquationNotFound`:
            If the equation with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Equation`:
            The retrieved equation.
        """

        return Equation.model_validate(self.items.get_by_pk({"id": id}))

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
        """Deletes a equation.

        Parameters
        ----------
        id : int
            The unique integer id of the equation.

        Raises
        ------
        :class:`EquationNotFound`:
            If the equation with `id` does not exist.
        :class:`EquationDeletionPrevented`:
            If the equation with `id` is used in the database, preventing it's deletion.
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
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id}/data", methods=["POST"]))
    def add_data(self, id: int, data: SerializableDataFrame | dict[str, Any]) -> None:
        """Adds data to an Equation.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Equation's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Equation.data`, existing values will be
        overwritten.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Equation`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        :class:`OptimizationItemUsageError`:
            - If values are missing, `None`, or `NaN`
            - If values are not allowed based on constraints to `Indexset`s
            - If rows are not unique
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        None
        """

        if isinstance(data, dict):
            try:
                data = pd.DataFrame.from_dict(data=data)
            except ValueError as e:
                raise EquationDataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        missing_columns = set(["levels", "marginals"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                f"Equation.data must include the column(s): "
                f"{', '.join(sorted(missing_columns))}!"
            )

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

    @procedure(Http(path="/{id}/data", methods=["DELETE"]))
    def remove_data(
        self, id: int, data: SerializableDataFrame | dict[str, Any] | None = None
    ) -> None:
        """Removes data from an Equation.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Equation`.
        data : dict[str, Any] | pandas.DataFrame, optional
            The data to be removed. If specified, remove only specific data. This must
            specify all indexed columns. All other keys/columns are ignored. Otherwise,
            remove all data (the default).

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

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

    @procedure(Http(methods=["PATCH"]))
    def list(self, **kwargs: Unpack[EquationFilter]) -> list[Equation]:
        r"""Lists equations by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `EquationFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Equation`]:
            List of equations.
        """
        return [Equation.model_validate(i) for i in self.items.list(values=kwargs)]

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
        self, pagination: Pagination, **kwargs: Unpack[EquationFilter]
    ) -> PaginatedResult[List[Equation]]:
        return PaginatedResult(
            results=[
                Equation.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=["PATCH"]))
    def tabulate(self, **kwargs: Unpack[EquationFilter]) -> SerializableDataFrame:
        r"""Tabulates equations by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `EquationFilter`.

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
        self, pagination: Pagination, **kwargs: Unpack[EquationFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=["POST"]))
    def revert(self, transaction__id: int, run__id: int) -> None:
        raise NotImplementedError
