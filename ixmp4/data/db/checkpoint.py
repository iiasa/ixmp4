import pandas as pd
from typing_extensions import Unpack

from ixmp4 import db
from ixmp4.data.auth.decorators import guard
from ixmp4.db import filters
from ixmp4.db.filters import BaseFilter

from .. import abstract, types
from . import base


class EnumerateKwargs(
    abstract.annotations.HasRunIdFilter,
    abstract.annotations.HasTransactionIdFilter,
    total=False,
):
    _filter: BaseFilter


class Checkpoint(base.BaseModel):
    run__id: types.Integer = db.Column(
        db.Integer,
        db.ForeignKey("run.id"),
        nullable=False,
        index=True,
    )
    transaction__id: types.Integer = db.Column(
        db.Integer,
        db.ForeignKey("transaction.id"),
        nullable=False,
        index=True,
    )
    message: types.String


class CheckpointFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    sqla_model = Checkpoint

    run__id: filters.Integer
    transaction__id: filters.Integer
    message: filters.String

    def join(
        self, exc: db.sql.Select[tuple[Checkpoint]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Checkpoint]]:
        return exc


class CheckpointRepository(
    base.Creator[Checkpoint],
    base.Deleter[Checkpoint],
    base.Enumerator[Checkpoint],
    abstract.CheckpointRepository,
):
    model_class = Checkpoint
    filter_class = CheckpointFilter

    def add(self, run__id: int, transaction__id: int, message: str) -> Checkpoint:
        checkpoint = Checkpoint(
            run__id=run__id, transaction__id=transaction__id, message=message
        )
        self.session.add(checkpoint)
        return checkpoint

    @guard("edit")
    def create(self, run__id: int, message: str) -> Checkpoint:
        latest_transaction = self.backend.runs.get_latest_transaction()
        # permission check
        run = self.backend.runs.get_by_id(run__id, _access_type="edit")

        return super().create(run.id, latest_transaction.id, message)

    @guard("edit")
    def delete(self, id: int) -> None:
        return super().delete(id)

    @guard("view")
    def get_by_id(self, id: int) -> Checkpoint:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Checkpoint.NotFound(id=id)

        return obj

    @guard("view")
    def list(self, /, **kwargs: Unpack[EnumerateKwargs]) -> list[Checkpoint]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)
