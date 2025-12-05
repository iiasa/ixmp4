from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.services import DirectTransport, Service, paginated_procedure, procedure

from .db import VariableDocs
from .dto import Variable
from .exceptions import (
    VariableNotFound,
)
from .filter import VariableFilter
from .repositories import (
    ItemRepository,
    PandasRepository,
    VersionRepository,
)


class VariableService(DocsService, Service):
    router_prefix = "/iamc/variables"
    router_tags = ["iamc", "variables"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    versions: VersionRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.versions = VersionRepository(self.executor)

        DocsService.__init_direct__(self, transport, docs_model=VariableDocs)

    @procedure(methods=["POST"])
    def create(self, name: str) -> Variable:
        """Creates a variable.

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`VariableNotUnique`:
            If the variable with `name` is not unique.


        Returns
        -------
        :class:`Variable`:
            The created variable.
        """
        self.items.create({"name": name, **self.get_creation_info()})
        return Variable.model_validate(self.items.get({"name": name}))

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["DELETE"])
    def delete_by_id(self, id: int) -> None:
        """Deletes a variable.

        Parameters
        ----------
        id : int
            The unique integer id of the variable.

        Raises
        ------
        :class:`VariableNotFound`:
            If the variable with `id` does not exist.
        :class:`VariableDeletionPrevented`:
            If the variable with `id` is used in the database, preventing it's deletion.
        """
        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(methods=["POST"])
    def get_by_name(self, name: str) -> Variable:
        """Retrieves a variable by its name.

        Parameters
        ----------
        name : str
            The unique name of the variable.

        Raises
        ------
        :class:`VariableNotFound`:
            If the variable with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Variable`:
            The retrieved variable.
        """
        return Variable.model_validate(self.items.get({"name": name}))

    @get_by_name.auth_check()
    def get_by_name_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["GET"])
    def get_by_id(self, id: int) -> Variable:
        """Retrieves a variable by its id.

        Parameters
        ----------
        id : int
            The integer id of the variable.

        Raises
        ------
        :class:`ixmp4.data.abstract.Variable.NotFound`:
            If the variable with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Variable`:
            The retrieved variable.
        """
        return Variable.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    def get_or_create(self, name: str) -> Variable:
        try:
            return self.get_by_name(name)
        except VariableNotFound:
            return self.create(name)

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[VariableFilter]) -> list[Variable]:
        r"""Lists variables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `VariableFilter`.

        Returns
        -------
        Iterable[:class:`Variable`]:
            List of variables.
        """

        return [Variable.model_validate(i) for i in self.items.list(values=kwargs)]

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

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[VariableFilter]) -> SerializableDataFrame:
        r"""Tabulates variables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `VariableFilter`.

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
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[VariableFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
