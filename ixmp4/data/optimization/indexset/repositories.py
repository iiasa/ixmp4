import logging
from typing import Any

import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ExtendedTarget, ModelTarget

from ixmp4.data.base.repository import AuthRepository

from .db import IndexSet, IndexSetData, IndexSetDataVersion, IndexSetVersion
from .exceptions import (
    IndexSetDataInvalid,
    IndexSetDataNotFound,
    IndexSetDataNotUnique,
    IndexSetNotFound,
    IndexSetNotUnique,
)
from .filter import IndexSetDataVersionFilter, IndexSetFilter, IndexSetVersionFilter
from .type import Type

logger = logging.getLogger(__name__)


class IndexSetAuthRepository(AuthRepository[IndexSet | IndexSetVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(IndexSet.run__id.in_(run_exc))


class IndexSetVersionAuthRepository(AuthRepository[IndexSetVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(IndexSetVersion.run__id.in_(run_exc))


class IndexSetDataVersionAuthRepository(AuthRepository[IndexSetDataVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        if run_exc is None:
            return exc
        return exc.where(
            IndexSetDataVersion.indexset.has(IndexSetVersion.run__id.in_(run_exc))
        )


class ItemRepository(IndexSetAuthRepository, BaseItemRepository[IndexSet]):
    NotFound = IndexSetNotFound
    NotUnique = IndexSetNotUnique
    target = ModelTarget(IndexSet)
    filter = Filter(IndexSetFilter, IndexSet)

    def check_type(
        self, id: int, data: list[float] | list[int] | list[str]
    ) -> Type | None:
        item = self.get_by_pk({"id": id})

        if item.data_type is None:
            data_type = None
        else:
            data_type = Type(item.data_type)

        for data_item in data:
            item_type = type(data_item)
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
            .where(IndexSet.id == id)
            .where(~IndexSet.data_entries.any())
            .values(data_type=None)
        )

        with self.wrap_executor_exception():
            with self.executor.update(exc) as result:
                return result


class PandasRepository(IndexSetAuthRepository, BasePandasRepository):
    target = ModelTarget(IndexSet)
    filter = Filter(IndexSetFilter, IndexSet)


class IndexSetDataItemRepository(BaseItemRepository[IndexSetData]):
    NotFound = IndexSetDataNotFound
    NotUnique = IndexSetDataNotUnique
    target = ModelTarget(IndexSetData)

    def add(self, indexset_id: int, data: list[str]) -> None:
        exc = sa.insert(IndexSetData).values(indexset__id=indexset_id)
        with self.wrap_executor_exception():
            with self.executor.insert_many(exc, [{"value": d} for d in data]):
                return None

    def remove(self, indexset_id: int, data: list[str]) -> int | None:
        exc = sa.delete(IndexSetData).where(IndexSetData.indexset__id == indexset_id)
        exc = exc.where(IndexSetData.value.in_(data))
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


class VersionRepository(IndexSetVersionAuthRepository, BasePandasRepository):
    NotFound = IndexSetNotFound
    NotUnique = IndexSetNotUnique
    target = ModelTarget(IndexSetVersion)
    filter = Filter(IndexSetVersionFilter, IndexSetVersion)


class DataVersionRepository(IndexSetDataVersionAuthRepository, BasePandasRepository):
    NotFound = IndexSetDataNotFound
    NotUnique = IndexSetDataNotUnique
    target = ExtendedTarget(
        IndexSetDataVersion,
        {
            "name": ((IndexSetDataVersion.indexset,), IndexSetVersion.name),
            "run__id": ((IndexSetDataVersion.indexset,), IndexSetVersion.run__id),
        },
    )
    filter = Filter(IndexSetDataVersionFilter, IndexSetDataVersion)
