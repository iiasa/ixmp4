from typing import List

from toolkit import db
from toolkit.exceptions import Unauthorized
from typing_extensions import Unpack

from ixmp4.rewrite.data.dataframe import SerializableDataFrame
from ixmp4.rewrite.data.pagination import PaginatedResult, Pagination
from ixmp4.rewrite.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .dto import Unit
from .filter import UnitFilter
from .repositories import ItemRepository, PandasRepository


class UnitService(Service):
    router_prefix = "/units"
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)

    @procedure(methods=["POST"])
    def create(self, name: str) -> Unit:
        """Creates a unit.

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`UnitNotUnique`:
            If the unit with `name` is not unique.


        Returns
        -------
        :class:`Unit`:
            The created unit.
        """
        self.auth_ctx.has_management_permission(self.platform, raise_exc=Unauthorized)

        self.items.create({"name": name})
        return Unit.model_validate(self.items.get({"name": name}))

    @procedure(methods=["DELETE"])
    def delete(self, id: int) -> None:
        """Deletes a unit.

        Parameters
        ----------
        id : int
            The unique integer id of the unit.

        Raises
        ------
        :class:`UnitNotFound`:
            If the unit with `id` does not exist.
        :class:`UnitDeletionPrevented`:
            If the unit with `id` is used in the database, preventing it's deletion.
        """
        self.auth_ctx.has_management_permission(self.platform, raise_exc=Unauthorized)

        self.items.delete_by_pk({"id": id})

    @procedure(methods=["POST"])
    def get_by_name(self, name: str) -> Unit:
        """Retrieves a unit by its name.

        Parameters
        ----------
        name : str
            The unique name of the unit.

        Raises
        ------
        :class:`UnitNotFound`:
            If the unit with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Unit`:
            The retrieved unit.
        """
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Unit.model_validate(self.items.get({"name": name}))

    @procedure(methods=["POST"])
    def get_by_id(self, id: int) -> Unit:
        """Retrieves a unit by its id.

        Parameters
        ----------
        id : int
            The integer id of the unit.

        Raises
        ------
        :class:`ixmp4.data.abstract.Unit.NotFound`:
            If the unit with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Unit`:
            The retrieved unit.
        """
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Unit.model_validate(self.items.get_by_pk({"id": id}))

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[UnitFilter]) -> list[Unit]:
        r"""Lists units by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `UnitFilter`.

        Returns
        -------
        Iterable[:class:`Unit`]:
            List of units.
        """
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return [Unit.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[UnitFilter]
    ) -> PaginatedResult[List[Unit]]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return PaginatedResult(
            results=[
                Unit.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[UnitFilter]) -> SerializableDataFrame:
        r"""Tabulates units by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `UnitFilter`.

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
        self, pagination: Pagination, **kwargs: Unpack[UnitFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
