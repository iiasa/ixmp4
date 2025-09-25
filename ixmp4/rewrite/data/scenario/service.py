from typing import List

import pandas as pd
from toolkit import db
from toolkit.exceptions import Unauthorized
from typing_extensions import Unpack

from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .dto import Scenario
from .filter import ScenarioFilter
from .repositories import ItemRepository, PandasRepository


class ScenarioService(Service):
    router_prefix = "/scenarios"
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)

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
        self.auth_ctx.has_management_permission(self.platform, raise_exc=Unauthorized)

        self.items.create({"name": name})
        return Scenario.model_validate(self.items.get({"name": name}))

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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Scenario.model_validate(self.items.get({"name": name}))

    @procedure(methods=["POST"])
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Scenario.model_validate(self.items.get_by_pk({"id": id}))

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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return [Scenario.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[ScenarioFilter]
    ) -> PaginatedResult[List[Scenario]]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

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
    def tabulate(self, **kwargs: Unpack[ScenarioFilter]) -> pd.DataFrame:
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return self.pandas.tabulate(values=kwargs)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[ScenarioFilter]
    ) -> PaginatedResult[pd.DataFrame]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
