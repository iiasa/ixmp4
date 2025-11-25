from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.exceptions import Forbidden
from ixmp4.services import (
    DirectTransport,
    Service,
    paginated_procedure,
)

from .db import VariableDocs
from .dto import Variable
from .filter import VariableFilter
from .repositories import ItemRepository, PandasRepository, VersionPandasRepository


class VariableService(DocsService, Service):
    router_prefix = "/iamc/variables"
    router_tags = ["iamc", "variables"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    pandas_versions: VersionPandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.pandas_versions = VersionPandasRepository(self.executor)

        DocsService.__init_direct__(self, transport, docs_model=VariableDocs)

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
