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

from .dto import Checkpoint
from .filter import CheckpointFilter
from .repositories import ItemRepository, PandasRepository


class CheckpointService(Service):
    router_prefix = "/checkpoints"
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)

    @procedure(methods=["POST"])
    def create(self, name: str) -> Checkpoint:
        """Creates a checkpoint.

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`CheckpointNotUnique`:
            If the checkpoint with `name` is not unique.


        Returns
        -------
        :class:`Checkpoint`:
            The created checkpoint.
        """
        # TODO: check run permission
        self.auth_ctx.has_edit_permission(self.platform, raise_exc=Unauthorized)

        self.items.create({"name": name})
        return Checkpoint.model_validate(self.items.get({"name": name}))

    @procedure(methods=["DELETE"])
    def delete(self, id: int) -> None:
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
        # TODO: check run permission
        self.auth_ctx.has_edit_permission(self.platform, raise_exc=Unauthorized)

        self.items.delete_by_pk({"id": id})

    @procedure(methods=["POST"])
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
        # TODO: check run permission
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return Checkpoint.model_validate(self.items.get_by_pk({"id": id}))

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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return [Checkpoint.model_validate(i) for i in self.items.list(values=kwargs)]

    @list.paginated()
    def paginated_list(
        self, pagination: Pagination, **kwargs: Unpack[CheckpointFilter]
    ) -> PaginatedResult[List[Checkpoint]]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

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
    def tabulate(self, **kwargs: Unpack[CheckpointFilter]) -> pd.DataFrame:
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
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return self.pandas.tabulate(values=kwargs)

    @tabulate.paginated()
    def paginated_tabulate(
        self, pagination: Pagination, **kwargs: Unpack[CheckpointFilter]
    ) -> PaginatedResult[pd.DataFrame]:
        self.auth_ctx.has_view_permission(self.platform, raise_exc=Unauthorized)

        return PaginatedResult(
            results=self.pandas.tabulate(
                values=kwargs, limit=pagination.limit, offset=pagination.offset
            ),
            total=self.pandas.count(values=kwargs),
            pagination=pagination,
        )
