import logging

import sqlalchemy as sa
from toolkit import db

from ixmp4.rewrite.exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)

from .db import IndexSet, IndexSetData, IndexSetVersion
from .filter import IndexSetFilter
from .type import Type

logger = logging.getLogger(__name__)


@registry.register()
class IndexSetNotFound(NotFound):
    pass


@registry.register()
class IndexSetNotUnique(NotUnique):
    pass


@registry.register()
class IndexSetDeletionPrevented(DeletionPrevented):
    pass


@registry.register()
class IndexSetDataInvalid(OptimizationDataValidationError):
    pass


@registry.register()
class IndexSetDataNotFound(NotFound, IndexSetDataInvalid):
    pass


@registry.register()
class IndexSetDataNotUnique(NotUnique, IndexSetDataInvalid):
    pass


class ItemRepository(db.r.ItemRepository[IndexSet]):
    NotFound = IndexSetNotFound
    NotUnique = IndexSetNotUnique
    target = db.r.ModelTarget(IndexSet)
    filter = db.r.Filter(IndexSetFilter, IndexSet)

    def check_type(
        self, id: int, data: list[float] | list[int] | list[str]
    ) -> Type | None:
        item = self.get_by_pk({"id": id})

        if item.data_type is None:
            data_type = None
        else:
            data_type = Type(item.data_type)

        for item in data:
            item_type = type(item)
            if data_type is None:
                data_type = Type.from_pytype(item_type)
                if data_type is None:
                    raise IndexSetDataInvalid(
                        f"Could not determine type for IndexSet data items: {data}"
                    )

            elif data_type != Type.from_pytype(item_type):
                raise IndexSetDataInvalid(
                    f"Could not determine type for IndexSet data items: {data}"
                )

        return data_type

    def reset_type(self, id: int) -> int | None:
        exc = (
            self.target.update_statement()
            .where((IndexSet.id == id) and (~IndexSet.data_entries.any()))
            .values(data_type=None)
        )

        with self.wrap_executor_exception():
            with self.executor.update(exc) as result:
                return result


class PandasRepository(db.r.PandasRepository):
    target = db.r.ModelTarget(IndexSet)
    filter = db.r.Filter(IndexSetFilter, IndexSet)


class IndexSetDataItemRepository(db.r.ItemRepository[IndexSetData]):
    NotFound = IndexSetDataNotFound
    NotUnique = IndexSetDataNotUnique
    target = db.r.ModelTarget(IndexSetData)

    def add(self, indexset_id: int, data: list[str]) -> None:
        exc = sa.insert(IndexSetData).values(indexset__id=indexset_id)
        with self.wrap_executor_exception():
            with self.executor.insert_many(exc, [{"value": d} for d in data]):
                return None

    def remove(self, indexset_id: int, data: list[str]) -> int | None:
        exc = sa.delete(IndexSetData).where(
            IndexSetData.indexset__id == indexset_id,
            IndexSetData.value.in_(data),
        )
        with self.wrap_executor_exception():
            with self.executor.delete(exc) as result:
                if result == 0:
                    logger.info(
                        f"No data were removed! Are {data} "
                        f"registered to IndexSet {indexset_id}?"
                    )
                    return None
                elif result != len(data):
                    logger.info(
                        "Not all items in `data` were "
                        f"registered for IndexSet {indexset_id}!"
                    )

                return result


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = IndexSetNotFound
    NotUnique = IndexSetNotUnique
    target = db.r.ModelTarget(IndexSetVersion)
    filter = db.r.Filter(IndexSetFilter, IndexSetVersion)
