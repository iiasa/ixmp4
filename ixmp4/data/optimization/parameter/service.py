from typing import Any, List

import pandas as pd
from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.optimization.base.service import IndexSetAssociatedService
from ixmp4.data.optimization.indexset.repositories import (
    ItemRepository as IndexSetRepository,
)
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.unit.repositories import ItemRepository as UnitRepository
from ixmp4.data.unit.repositories import UnitNotFound
from ixmp4.exceptions import Forbidden, OptimizationItemUsageError
from ixmp4.services import (
    DirectTransport,
    paginated_procedure,
    procedure,
)

from .db import ParameterDocs
from .dto import Parameter
from .filter import ParameterFilter
from .repositories import (
    AssociationRepository,
    ItemRepository,
    PandasRepository,
    ParameterDataInvalid,
    VersionPandasRepository,
)


class ParameterService(DocsService, IndexSetAssociatedService):
    router_prefix = "/optimization/parameters"
    router_tags = ["optimization", "parameters"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    pandas_versions: VersionPandasRepository

    associations: AssociationRepository
    indexsets: IndexSetRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.pandas_versions = VersionPandasRepository(self.executor)
        self.units = UnitRepository(self.executor)
        self.associations = AssociationRepository(self.executor)
        self.indexsets = IndexSetRepository(self.executor)
        DocsService.__init_direct__(self, transport, docs_model=ParameterDocs)

    @procedure(methods=["POST"])
    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        """Creates a Parameter.

        Each column of the Parameter needs to be constrained to an existing
        :class:`ixmp4.data.abstract.optimization.IndexSet`. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Parameter is
            defined.
        name : str
            The unique name of the Parameter.
        constrained_to_indexsets : list[str]
            List of :class:`ixmp4.data.abstract.optimization.IndexSet` names that define
            the allowed contents of the Parameter's columns.
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.

        Raises
        ------
        :class:`ParameterNotUnique`:
            If the parameter is not unique.
        :class:`OptimizationItemUsageError`:
            If the parameter arguments are not valid.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Parameter`:
            The created parameter.
        """
        if column_names:
            self.check_column_args(
                name, "Parameter", constrained_to_indexsets, column_names
            )
        else:
            column_names = [None] * len(constrained_to_indexsets)

        self.items.create({"name": name, "run__id": run_id, **self.get_creation_info()})
        db_par = self.items.get({"name": name, "run__id": run_id})

        if constrained_to_indexsets:
            for idxset_name, col_name in zip(constrained_to_indexsets, column_names):
                indexset = self.indexsets.get({"name": idxset_name, "run__id": run_id})
                self.associations.create(
                    {
                        "parameter__id": db_par.id,
                        "indexset__id": indexset.id,
                        "column_name": col_name,
                    }
                )

        return Parameter.model_validate(db_par)

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

    @procedure(methods=["POST"])
    def get(self, run_id: int, name: str) -> Parameter:
        """Retrieves a parameter by its name and run_id.

        Parameters
        ----------
        run_id : int
            The unique name of the parameter.

        name : str
            The unique name of the parameter.

        Raises
        ------
        :class:`ParameterNotFound`:
            If the parameter with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Parameter`:
            The retrieved parameter.
        """
        return Parameter.model_validate(
            self.items.get({"name": name, "run__id": run_id})
        )

    @get.auth_check()
    def get_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["GET"])
    def get_by_id(self, id: int) -> Parameter:
        """Retrieves a parameter by its id.

        Parameters
        ----------
        id : int
            The integer id of the parameter.

        Raises
        ------
        :class:`ParameterNotFound`:
            If the parameter with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Parameter`:
            The retrieved parameter.
        """

        return Parameter.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["DELETE"])
    def delete_by_id(self, id: int) -> None:
        """Deletes a parameter.

        Parameters
        ----------
        id : int
            The unique integer id of the parameter.

        Raises
        ------
        :class:`ParameterNotFound`:
            If the parameter with `id` does not exist.
        :class:`ParameterDeletionPrevented`:
            If the parameter with `id` is used in the database, preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """
        self.items.delete_associations(id)
        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/data", methods=["POST"])
    def add_data(self, id: int, data: dict[str, Any] | SerializableDataFrame) -> None:
        r"""Adds data to a Parameter.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Parameter's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Parameter.data`, existing values will be
        overwritten.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Parameter`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        :class:`ixmp4.core.exceptions.OptimizationDataValidationError`:
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
                raise ParameterDataInvalid(str(e)) from e

        if data.empty:
            return  # nothing to do

        missing_columns = set(["values", "units"]) - set(data.columns)
        if missing_columns:
            raise OptimizationItemUsageError(
                "Parameter.data must include the column(s): "
                f"{', '.join(sorted(missing_columns))}!"
            )

        # TODO Move error handling to facade
        # Can use a set for now, need full column if we care about order
        for unit_name in set(data["units"]):
            try:
                self.units.get({"name": unit_name})
            except UnitNotFound as e:
                # TODO Add a helpful hint on how to check defined Units
                # TODO Move to facade
                raise UnitNotFound(
                    message=f"'{unit_name}' is not defined for this Platform!"
                ) from e

        self.items.add_data(id, data)

    @add_data.auth_check()
    def add_data_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/data", methods=["DELETE"])
    def remove_data(
        self, id: int, data: dict[str, Any] | SerializableDataFrame | None = None
    ) -> None:
        r"""Removes data from a Parameter.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Parameter`.
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
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[ParameterFilter]) -> list[Parameter]:
        r"""Lists parameters by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `ParameterFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Parameter`]:
            List of parameters.
        """
        return [Parameter.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.auth_check()
    def list_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[ParameterFilter]
    ) -> PaginatedResult[List[Parameter]]:
        return PaginatedResult(
            results=[
                Parameter.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[ParameterFilter]) -> SerializableDataFrame:
        r"""Tabulates parameters by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `ParameterFilter`.

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
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[ParameterFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
