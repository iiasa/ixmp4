from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.docs.service import DocsService
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.exceptions import Forbidden
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .db import ScenarioDocs
from .dto import Scenario
from .filter import ScenarioFilter
from .repositories import ItemRepository, PandasRepository


class ScenarioService(DocsService, Service):
    router_prefix = "/scenarios"
    router_tags = ["scenarios"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        DocsService.__init_direct__(self, transport, docs_model=ScenarioDocs)

    @procedure(methods=["POST"])
    def create(self, name: str) -> Scenario:
        """Creates a scenario.

        Parameters
        ----------
        name : str
            The name of the scenario.

        Raises
        ------
        :class:`ScenarioNotUnique`:
            If the scenario with `name` is not unique.


        Returns
        -------
        :class:`Scenario`:
            The created scenario.
        """

        self.items.create({"name": name})
        return Scenario.model_validate(self.items.get({"name": name}))

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        name: str,
    ) -> None:
        auth_ctx.has_management_permission(platform, raise_exc=Forbidden)

    @procedure(methods=["POST"])
    def get_by_name(self, name: str) -> Scenario:
        """Retrieves a scenario by its name.

        Parameters
        ----------
        name : str
            The unique name of the scenario.

        Raises
        ------
        :class:`ScenarioNotFound`:
            If the scenario with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Scenario`:
            The retrieved scenario.
        """

        return Scenario.model_validate(self.items.get({"name": name}))

    @get_by_name.auth_check()
    def get_by_name_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        name: str,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["GET"])
    def get_by_id(self, id: int) -> Scenario:
        """Retrieves a scenario by its id.

        Parameters
        ----------
        id : int
            The integer id of the scenario.

        Raises
        ------
        :class:`ixmp4.data.abstract.Scenario.NotFound`:
            If the scenario with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Scenario`:
            The retrieved scenario.
        """

        return Scenario.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        id: int,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[ScenarioFilter]) -> list[Scenario]:
        r"""Lists scenarios by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `ScenarioFilter`.

        Returns
        -------
        Iterable[:class:`Scenario`]:
            List of scenarios.
        """
        return [Scenario.model_validate(i) for i in self.items.list(values=kwargs)]

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
        self, pagination: Pagination, **kwargs: Unpack[ScenarioFilter]
    ) -> PaginatedResult[List[Scenario]]:
        return PaginatedResult(
            results=[
                Scenario.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[ScenarioFilter]) -> SerializableDataFrame:
        r"""Tabulates scenarios by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `ScenarioFilter`.

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
        self, pagination: Pagination, **kwargs: Unpack[ScenarioFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
