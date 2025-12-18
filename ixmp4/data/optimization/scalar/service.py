from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.docs.service import DocsService
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.run.repositories import ItemRepository as RunRepository
from ixmp4.data.unit.repositories import ItemRepository as UnitRepository
from ixmp4.services import DirectTransport, GetByIdService, Http, procedure

from .db import ScalarDocs
from .dto import Scalar
from .filter import ScalarFilter
from .repositories import ItemRepository, PandasRepository, VersionRepository


class ScalarService(DocsService, GetByIdService):
    router_prefix = "/optimization/scalars"
    router_tags = ["optimization", "scalars"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository
    versions: VersionRepository

    runs: RunRepository
    indexsets: UnitRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor, **self.get_auth_kwargs(transport))
        self.pandas = PandasRepository(self.executor, **self.get_auth_kwargs(transport))
        self.versions = VersionRepository(self.executor)
        self.units = UnitRepository(self.executor)
        self.runs = RunRepository(self.executor)

        DocsService.__init_direct__(self, transport, docs_model=ScalarDocs)

    @procedure(Http(path="/", methods=("POST",)))
    def create(
        self, run_id: int, name: str, value: float | int, unit_name: str
    ) -> Scalar:
        """Creates a scalar.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Scalar is
            defined.
        name : str
            The name of the Scalar.
        value : float
            The value of the Scalar.
        unit_name : str
            The name of the :class:`ixmp4.data.abstract.Unit` for which this Scalar is
            defined.

        Raises
        ------
        :class:`ScalarNotUnique`:
            If the scalar is not unique.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Scalar`:
            The created scalar.
        """

        unit_id = self.units.get({"name": unit_name}).id

        self.items.create(
            {
                "name": name,
                "run__id": run_id,
                "value": value,
                "unit__id": unit_id,
                **self.get_creation_info(),
            }
        )
        db_sca = self.items.get({"name": name, "run__id": run_id})
        return Scalar.model_validate(db_sca)

    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        /,
        run_id: int,
        name: str,
        value: float | int,
        unit_name: str,
    ) -> None:
        run = self.runs.get_by_pk({"id": run_id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(methods=("POST",)))
    def get(self, run_id: int, name: str) -> Scalar:
        """Retrieves a scalar by its name and run_id.

        Parameters
        ----------
        run_id : int
            The unique name of the scalar.

        name : str
            The unique name of the scalar.

        Raises
        ------
        :class:`ScalarNotFound`:
            If the scalar with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Scalar`:
            The retrieved scalar.
        """
        return Scalar.model_validate(self.items.get({"name": name, "run__id": run_id}))

    @get.auth_check()
    def get_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        run_id: int,
        name: str,
    ) -> None:
        run = self.runs.get_by_pk({"id": run_id})
        auth_ctx.has_view_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(path="/{id:int}/", methods=("GET",)))
    def get_by_id(self, id: int) -> Scalar:
        """Retrieves a scalar by its id.

        Parameters
        ----------
        id : int
            The integer id of the scalar.

        Raises
        ------
        :class:`ScalarNotFound`:
            If the scalar with `id` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`Scalar`:
            The retrieved scalar.
        """

        return Scalar.model_validate(self.items.get_by_pk({"id": id}))

    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @procedure(Http(path="/{id:int}/", methods=("DELETE",)))
    def delete_by_id(self, id: int) -> None:
        """Deletes a scalar.

        Parameters
        ----------
        id : int
            The unique integer id of the scalar.

        Raises
        ------
        :class:`ScalarNotFound`:
            If the scalar with `id` does not exist.
        :class:`ScalarDeletionPrevented`:
            If the scalar with `id` is used in the database, preventing it's deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """

        self.items.delete_by_pk({"id": id})

    @delete_by_id.auth_check()
    def delete_by_id_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol, /, id: int
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)
        item = self.items.get_by_pk({"id": id})
        run = self.runs.get_by_pk({"id": item.run__id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(path="/{id:int}/", methods=("POST",)))
    def update_by_id(
        self, id: int, value: float | int | None = None, unit_name: str | None = None
    ) -> Scalar:
        """Updates a Scalar.

        Parameters
        ----------
        id : int
            The integer id of the Scalar.
        value : float, optional
            The value of the Scalar.
        unit_id : int, optional
            The id of the :class:`ixmp4.data.abstract.Unit` for which this Scalar is
            defined.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Scalar`:
            The updated Scalar.
        """

        values: dict[str, Any] = {"id": id}

        if value is not None:
            values["value"] = value
        if unit_name is not None:
            values["unit__id"] = self.units.get({"name": unit_name}).id

        self.items.update_by_pk(values)
        return Scalar.model_validate(self.items.get_by_pk({"id": id}))

    @update_by_id.auth_check()
    def update_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        /,
        id: int,
        value: float | int | None = None,
        unit_name: str | None = None,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)
        item = self.items.get_by_pk({"id": id})
        run = self.runs.get_by_pk({"id": item.run__id})
        auth_ctx.has_edit_permission(
            platform, models=[run.model.name], raise_exc=Forbidden
        )

    @procedure(Http(methods=("PATCH",)))
    def list(self, **kwargs: Unpack[ScalarFilter]) -> list[Scalar]:
        r"""Lists scalars by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter scalars as specified in `ScalarFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        list[:class:`Scalar`]:
            List of scalars.
        """
        return [Scalar.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.auth_check()
    def list_auth_check(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[ScalarFilter]
    ) -> PaginatedResult[List[Scalar]]:
        return PaginatedResult(
            results=[
                Scalar.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @procedure(Http(methods=("PATCH",)))
    def tabulate(self, **kwargs: Unpack[ScalarFilter]) -> SerializableDataFrame:
        r"""Tabulates scalars by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter scalars as specified in `ScalarFilter`.

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
        self, pagination: Pagination, **kwargs: Unpack[ScalarFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult[SerializableDataFrame](
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
