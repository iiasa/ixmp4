from typing import Any, ClassVar, Generic, ParamSpec

import pandas as pd
import sqlalchemy as sa
from toolkit import db

from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.base.db import BaseModel

from .model import BaseVersionModel, Operation
from .transaction import TransactionRepository

Params = ParamSpec("Params")


class ReverterRepository(db.r.PandasRepository, Generic[Params]):
    target: db.r.ModelTarget[BaseModel]
    version_target: ClassVar[db.r.ModelTarget[BaseVersionModel]]

    transactions: TransactionRepository
    versioned_columns: sa.ColumnCollection[str, sa.ColumnElement[Any]]
    revert_op_label = "revert_operation_type"

    def __init__(self, executor: db.r.SessionExecutor):
        super().__init__(executor)

        if getattr(self, "version_target", None) is None:
            raise ProgrammingError(
                f"`{self.__class__.__name__}` requires a `version_target` to function."
            )

        self.transactions = TransactionRepository(self.executor)

        self.versioned_columns = sa.ColumnCollection()

        for column in self.target.table.columns:
            version_column = self.version_target.table.columns.get(column.key)
            if version_column is not None and (
                str(column.type) == str(version_column.type)
            ):
                self.versioned_columns.add(column)

    def select_versions(
        self,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> sa.Select[Any]:
        return self.version_target.select_statement()

    def where_valid_at_tx(self, exc: sa.Select[Any], tx_id: int) -> sa.Select[Any]:
        model_class = self.version_target.model_class

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

    def check_tx_ids(self, origin_tx_id: int, compare_tx_id: int) -> None:
        if origin_tx_id < compare_tx_id:
            raise ProgrammingError("`origin_tx_id` must be bigger than `compare_tx_id`")

    def select_deleted_versions(
        self,
        compare_tx_id: int,
        origin_exc: sa.Select[Any],
        compare_exc: sa.Select[Any],
    ) -> sa.Select[Any]:
        deleted_exc = self.version_target.select_statement()
        deleted_exc = deleted_exc.where(
            self.version_target.model_class.id.in_(compare_exc)
        )
        deleted_exc = deleted_exc.where(
            self.version_target.model_class.id.not_in(origin_exc)
        )
        deleted_exc = self.where_valid_at_tx(deleted_exc, compare_tx_id)
        return deleted_exc

    def select_updated_versions(
        self,
        compare_tx_id: int,
        origin_exc: sa.Select[Any],
        compare_exc: sa.Select[Any],
    ) -> sa.Select[Any]:
        updated_subq = self.version_target.select_statement()
        updated_subq = updated_subq.where(
            self.version_target.model_class.id.in_(compare_exc)
        )
        updated_subq = updated_subq.where(
            self.version_target.model_class.id.in_(origin_exc)
        )
        updated_subq = self.where_valid_at_tx(updated_subq, compare_tx_id)
        return updated_subq

    def select_inserted_versions(
        self, origin_tx_id: int, origin_exc: sa.Select[Any], compare_exc: sa.Select[Any]
    ) -> sa.Select[Any]:
        inserted_subq = self.version_target.select_statement()
        inserted_subq = inserted_subq.where(
            self.version_target.model_class.id.not_in(compare_exc)
        )
        inserted_subq = inserted_subq.where(
            self.version_target.model_class.id.in_(origin_exc)
        )
        inserted_subq = self.where_valid_at_tx(inserted_subq, origin_tx_id)
        return inserted_subq

    def tabulate_revert_ops(
        self,
        origin_tx_id: int,
        compare_tx_id: int,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> pd.DataFrame:
        """Tabulates the operations needed to revert from
        `origin_tx_id` to `compare_tx_id`. The 'revert_operation_type' column
        indicates which operation is needed to perform the rollback.
        Original version columns are also included (beware of the original
        'operation_type' column)."""
        self.check_tx_ids(origin_tx_id, compare_tx_id)

        id_col = self.version_target.model_class.id
        base_exc = self.select_versions(*args, **kwargs).with_only_columns(id_col)

        origin_exc = self.where_valid_at_tx(base_exc, origin_tx_id)
        compare_exc = self.where_valid_at_tx(base_exc, compare_tx_id)

        deleted_subq = self.select_deleted_versions(
            compare_tx_id, origin_exc, compare_exc
        ).add_columns(sa.literal(Operation.INSERT.value).label(self.revert_op_label))

        updated_subq = self.select_updated_versions(
            compare_tx_id, origin_exc, compare_exc
        ).add_columns(sa.literal(Operation.UPDATE.value).label(self.revert_op_label))

        inserted_subq = self.select_inserted_versions(
            origin_tx_id, origin_exc, compare_exc
        ).add_columns(sa.literal(Operation.DELETE.value).label(self.revert_op_label))

        exc = sa.union_all(deleted_subq, updated_subq, inserted_subq)
        return self.execute_select_tabulation(exc)

    def revert_constructive(
        self,
        origin_tx_id: int,
        compare_tx_id: int,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> None:
        self.check_tx_ids(origin_tx_id, compare_tx_id)

        base_exc = self.select_versions(*args, **kwargs).with_only_columns(
            self.version_target.model_class.id
        )
        origin_exc = self.where_valid_at_tx(base_exc, origin_tx_id)
        compare_exc = self.where_valid_at_tx(base_exc, compare_tx_id)

        # insert rows that were deleted since `compare_tx_id`
        deleted_cte = self.select_deleted_versions(
            compare_tx_id, origin_exc, compare_exc
        ).cte("insert_values")

        # update rows were updated, where data comes from 'compare'
        updated_cte = self.select_updated_versions(
            compare_tx_id, origin_exc, compare_exc
        ).cte("update_values")

        deleted_subq_columns = (deleted_cte.c[k] for k in self.versioned_columns.keys())
        insert_exc = self.target.insert_statement().from_select(
            self.versioned_columns.keys(),
            sa.select(*deleted_subq_columns),
        )
        self.executor.session.execute(insert_exc)

        update_map = {
            v: updated_cte.c[k] for k, v in self.versioned_columns.items() if k != "id"
        }
        update_exc = (
            self.target.update_statement()
            .add_cte(updated_cte)
            .where(self.target.model_class.id == updated_cte.c.id)
            .values(update_map)
        )

        self.executor.session.execute(update_exc)

    def revert_destructive(
        self,
        origin_tx_id: int,
        compare_tx_id: int,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> int | None:
        self.check_tx_ids(origin_tx_id, compare_tx_id)

        base_exc = self.select_versions(*args, **kwargs).with_only_columns(
            self.version_target.model_class.id
        )
        origin_exc = self.where_valid_at_tx(base_exc, origin_tx_id)
        compare_exc = self.where_valid_at_tx(base_exc, compare_tx_id)

        # delete rows that were inserted since `compare_tx_id`
        inserted_subq = self.select_inserted_versions(
            origin_tx_id, origin_exc, compare_exc
        ).with_only_columns(self.version_target.model_class.id)
        delete_exc = self.target.delete_statement().where(
            self.target.model_class.id.in_(inserted_subq)
        )

        with self.executor.delete(delete_exc) as result:
            return result

    def revert(
        self,
        tx_id: int,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> None:
        latest_tx = self.transactions.latest()
        self.revert_destructive(latest_tx.id, tx_id, *args, **kwargs)
        self.revert_constructive(latest_tx.id, tx_id, *args, **kwargs)
        self.executor.session.commit()


class Reverter(Generic[Params]):
    repo_classes: list[type[ReverterRepository[Params]]]

    def __init__(
        self,
        targets: list[type[ReverterRepository[Params]]],
    ) -> None:
        def sorted_index(repo_class: type[ReverterRepository[Params]]) -> int:
            return BaseModel.metadata.sorted_tables.index(repo_class.target.table)

        self.repo_classes = list(sorted(targets, key=sorted_index))

    def __call__(
        self,
        executor: db.r.SessionExecutor,
        tx_id: int,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> None:
        transactions = TransactionRepository(executor)
        origin_tx_id = transactions.latest().id
        targets: list[ReverterRepository[Params]] = [
            class_(executor) for class_ in self.repo_classes
        ]

        for target in reversed(targets):
            target.revert_destructive(origin_tx_id, tx_id, *args, **kwargs)
            executor.session.commit()

        for target in targets:
            target.revert_constructive(origin_tx_id, tx_id, *args, **kwargs)
            executor.session.commit()
