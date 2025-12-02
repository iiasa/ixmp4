from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.base_exceptions import NotFound, NotUnique, registry
from ixmp4.data.base.db import BaseModel


@registry.register()
class TransactionNotFound(NotFound):
    pass


@registry.register()
class TransactionNotUnique(NotUnique):
    pass


class Transaction(BaseModel):
    __tablename__ = "transaction"

    issued_at: db.t.DateTime = orm.mapped_column(sa.DateTime())


class TransactionRepository(db.r.ItemRepository[Transaction]):
    NotUnique = TransactionNotUnique
    NotFound = TransactionNotFound
    target = db.r.ModelTarget(Transaction)

    def latest(self) -> Transaction:
        exc = self.target.select_statement()
        # limit 1 to avoid the dbapi backend loading all rows
        exc = exc.order_by(Transaction.id.desc()).limit(1)

        try:
            with self.executor.select(exc) as result, self.expect_one_result():
                return self.target.get_single_item(result)
        except self.NotFound:
            result = self.create({"issued_at": datetime.now(tz=timezone.utc)})
            return self.get_by_pk({"id": result.inserted_primary_key.id})
