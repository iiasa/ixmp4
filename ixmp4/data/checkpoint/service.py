from typing import Any, List

from toolkit import db
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance
from typing_extensions import Unpack

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.dataframe import SerializableDataFrame
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.data.versions.transaction import TransactionRepository
from ixmp4.services import (
    DirectTransport,
    Service,
    paginated_procedure,
    procedure,
)

from .dto import Checkpoint
from .filter import CheckpointFilter
from .repositories import ItemRepository, PandasRepository


class CheckpointService(Service):
    router_prefix = "/checkpoints"
    router_tags = ["checkpoints"]

    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        self.transactions = TransactionRepository(self.executor)

    @procedure(methods=["POST"])
    def create(
        self, run__id: int, message: str, transaction__id: int | None = None
    ) -> Checkpoint:
        """Creates a checkpoint.

        Parameters
        ----------
        run__id : int
            Id of the run the checkpoint is connected to.
        message : str
            The checkpoints message.
        transaction__id : int, optional
            Id of the transaction the checkpoint references.

        Raises
        ------
        :class:`CheckpointNotUnique`:
            If the checkpoint with `name` is not unique.

        Returns
        -------
        :class:`Checkpoint`:
            The created checkpoint.
        """
        if transaction__id is None and self.get_dialect().name == "postgresql":
            # fill the latest transaction as default for
            # postgres dbs which support versioning
            transaction__id = self.transactions.latest().id

        result = self.items.create(
            {"run__id": run__id, "transaction__id": transaction__id, "message": message}
        )
        return Checkpoint.model_validate(
            self.items.get_by_pk({"id": result.inserted_primary_key.id})
        )

    # TODO: check run permission
    @create.auth_check()
    def create_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["DELETE"])
    def delete_by_id(self, id: int) -> None:
        """Deletes a checkpoint.

        Parameters
        ----------
        id : int
            The unique integer id of the checkpoint.

        Raises
        ------
        :class:`CheckpointNotFound`:
            If the checkpoint with `id` does not exist.
        """

        self.items.delete_by_pk({"id": id})

    # TODO: check run permission
    @delete_by_id.auth_check()
    def delete_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_edit_permission(platform, raise_exc=Forbidden)

    @procedure(path="/{id}/", methods=["GET"])
    def get_by_id(self, id: int) -> Checkpoint:
        """Retrieves a checkpoint by its id.

        Parameters
        ----------
        id : int
            The integer id of the checkpoint.

        Raises
        ------
        :class:`ixmp4.data.abstract.Checkpoint.NotFound`:
            If the checkpoint with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Checkpoint`:
            The retrieved checkpoint.
        """

        return Checkpoint.model_validate(self.items.get_by_pk({"id": id}))

    # TODO: check run permission
    @get_by_id.auth_check()
    def get_by_id_auth_check(
        self,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        auth_ctx.has_view_permission(platform, raise_exc=Forbidden)

    @paginated_procedure(methods=["PATCH"])
    def list(self, **kwargs: Unpack[CheckpointFilter]) -> list[Checkpoint]:
        r"""Lists checkpoints by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `CheckpointFilter`.

        Returns
        -------
        Iterable[:class:`Checkpoint`]:
            List of checkpoints.
        """

        return [Checkpoint.model_validate(i) for i in self.items.list(values=kwargs)]

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
        self, pagination: Pagination, **kwargs: Unpack[CheckpointFilter]
    ) -> PaginatedResult[List[Checkpoint]]:
        return PaginatedResult(
            results=[
                Checkpoint.model_validate(i)
                for i in self.items.list(
                    values=kwargs, limit=pagination.limit, offset=pagination.offset
                )
            ],
            total=self.items.count(values=kwargs),
            pagination=pagination,
        )

    @paginated_procedure(methods=["PATCH"])
    def tabulate(self, **kwargs: Unpack[CheckpointFilter]) -> SerializableDataFrame:
        r"""Tabulates checkpoints by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in `CheckpointFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - run__id
                - transcation__id
                - message
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
        self, pagination: Pagination, **kwargs: Unpack[CheckpointFilter]
    ) -> PaginatedResult[SerializableDataFrame]:
        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
