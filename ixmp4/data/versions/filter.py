from typing import Annotated, Any, TypedDict

import sqlalchemy as sa
from toolkit import db

from .model import Operation


def filter_by_valid_at_transaction(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: int,
    *,
    repo: db.r.BaseRepository[Any],
    **kwargs: Any,
) -> sa.Select[Any] | sa.Update | sa.Delete:
    tx_id_col = repo.target.table.c["transaction_id"]
    end_tx_id_col = repo.target.table.c["end_transaction_id"]
    op_type_col = repo.target.table.c["operation_type"]

    return exc.where(
        sa.and_(
            tx_id_col <= value,
            op_type_col != Operation.DELETE.value,
            sa.or_(
                end_tx_id_col > value,
                end_tx_id_col == sa.null(),
            ),
        )
    )


class VersionFilter(TypedDict):
    valid_at_transaction: Annotated[int, filter_by_valid_at_transaction]
