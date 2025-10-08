import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data.base.db import BaseModel
from ixmp4.rewrite.exceptions import NotFound, NotUnique, registry


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

        with self.executor.select(exc) as result, self.expect_one_result():
            return self.target.get_single_item(result)
