from typing import Any, Sequence

import pandas as pd
import sqlalchemy as sa
from toolkit import db
from toolkit.db.repository.base import Values

from ixmp4.core.exceptions import ProgrammingError

from .model import BaseVersionModel, Operation
from .transaction import TransactionRepository


class BaseVersionRepository(db.r.PandasRepository):
    target: db.r.ModelTarget[BaseVersionModel]
    transactions: TransactionRepository
    rollback_op_label = "rollback_operation_type"

    def __init__(self, *a: Any, **kw: Any):
        super().__init__(*a, **kw)
        self.transactions = TransactionRepository(self.executor)

    def where_valid_at_tx(self, exc: sa.Select[Any], tx_id: int) -> sa.Select[Any]:
        model_class = self.target.model_class

        return exc.where(
            sa.and_(
                model_class.transaction_id <= tx_id,
                model_class.operation_type != Operation.DELETE.value,
                sa.or_(
                    model_class.end_transaction_id > tx_id,
                    model_class.end_transaction_id == sa.null(),
                ),
            )
        )

    def tabulate_valid_at_tx(
        self,
        tx_id: int,
        values: Values | None = None,
        columns: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ):
        exc = self.select_for_values(
            values=values, columns=columns, limit=limit, offset=offset
        )
        exc = self.default_order_by(exc)
        exc = self.where_valid_at_tx(exc, tx_id)
        return self.execute_select_tabulation(exc)

    def tabulate_difference(
        self, origin_tx_id: int, compare_tx_id: int, filter_values: Values | None = None
    ) -> pd.DataFrame:
        """Tabulates the operations needed to roll back from
        `origin_tx_id` to `compare_tx_id`. The 'rollback_operation_type' column
        indicates which operation is needed to perform the rollback.
        Original version columns are also included (beware of the original
        'operation_type' column)."""

        if origin_tx_id < compare_tx_id:
            raise ProgrammingError("`origin_tx_id` must be bigger than `compare_tx_id`")

        id_col = self.target.model_class.id
        base_exc = self.select_for_values(filter_values, columns=["id"])

        origin_exc = self.where_valid_at_tx(base_exc, origin_tx_id)
        compare_exc = self.where_valid_at_tx(base_exc, compare_tx_id)

        # insert rows that are in 'compare' but not 'origin' (deleted)
        insert_exc = self.target.select_statement().add_columns(
            sa.literal(Operation.INSERT.value).label(self.rollback_op_label)
        )
        insert_exc = insert_exc.where(id_col.in_(compare_exc))
        insert_exc = insert_exc.where(id_col.not_in(origin_exc))
        insert_exc = self.where_valid_at_tx(insert_exc, compare_tx_id)

        # delete rows that are in 'origin' but not 'compare' (inserted)
        delete_exc = self.target.select_statement().add_columns(
            sa.literal(Operation.DELETE.value).label(self.rollback_op_label)
        )
        delete_exc = delete_exc.where(id_col.not_in(compare_exc))
        delete_exc = delete_exc.where(id_col.in_(origin_exc))
        delete_exc = self.where_valid_at_tx(delete_exc, origin_tx_id)

        # update rows that are in both, where row data comes from 'compare'
        update_exc = self.target.select_statement().add_columns(
            sa.literal(Operation.UPDATE.value).label(self.rollback_op_label)
        )
        update_exc = update_exc.where(id_col.in_(compare_exc))
        update_exc = update_exc.where(id_col.in_(origin_exc))
        update_exc = self.where_valid_at_tx(update_exc, compare_tx_id)

        exc = sa.union_all(insert_exc, delete_exc, update_exc)
        return self.execute_select_tabulation(exc)
