from typing import Annotated, Any

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository
from typing_extensions import TypedDict

from .model import Operation


def filter_by_valid_at_transaction(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: int,
    *,
    repo: BaseRepository[Any],
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


class VersionFilter(TypedDict, total=False):
    valid_at_transaction: Annotated[int, filter_by_valid_at_transaction]

    transaction_id: int
    transaction_id__gt: int
    transaction_id__gte: int
    transaction_id__lt: int
    transaction_id__lte: int

    end_transaction_id: int
    end_transaction_id__gt: int
    end_transaction_id__gte: int
    end_transaction_id__lt: int
    end_transaction_id__lte: int

    operation_type: int
