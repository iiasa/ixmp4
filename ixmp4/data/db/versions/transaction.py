from typing import Any

from sqlalchemy import DateTime

from ixmp4 import db
from ixmp4.core.exceptions import NotFound
from ixmp4.data import types

from .. import base


class Transaction(base.BaseModel):
    NotFound = NotFound
    __tablename__ = "transaction"

    issued_at: types.DateTime = db.Column(DateTime())


class TransactionRepository(
    base.Retriever[Transaction],
    base.Enumerator[Transaction],
):
    model_class = Transaction

    def select(self, **kwargs: Any) -> db.sql.Select[tuple[Transaction]]:
        return db.select(self.model_class).order_by(self.model_class.id.desc())

    def latest(self) -> Transaction:
        tx = self.session.execute(self.select()).scalars().first()
        if tx is None:
            raise Transaction.NotFound("No transactions in database.")
        return tx
