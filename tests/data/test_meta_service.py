import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.meta.dto import MetaValueType, RunMetaEntry
from ixmp4.data.meta.repositories import (
    RunMetaEntryNotFound,
)
from ixmp4.data.meta.service import RunMetaEntryService
from ixmp4.data.meta.type import Type
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


# TODO: Versions
class RunMetaEntryServiceTest(ServiceTest[RunMetaEntryService]):
    service_class = RunMetaEntryService
    test_entries: list[tuple[int, str, MetaValueType, Type]] = [
        (1, "Boolean", True, Type.BOOL),
        (2, "Float", 0.2, Type.FLOAT),
        (3, "Integer", 1, Type.INT),
        (4, "String", "Value", Type.STR),
    ]

    @property
    def test_entries_df(self):
        return pd.DataFrame(
            [[id, key, val, str(type_)] for id, key, val, type_ in self.test_entries],
            columns=["id", "key", "value", "dtype"],
        )

    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def run(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    def assert_value(self, meta: RunMetaEntry, val: MetaValueType):
        value_type = Type.from_pytype(type(val))
        assert Type(meta.dtype) == value_type
        col = Type.column_for_type(value_type)
        assert getattr(meta, col) == val
        assert meta.value == val


class TestRunMetaEntryCreateGet(RunMetaEntryServiceTest):
    def test_meta_create(
        self,
        service: RunMetaEntryService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        for entry in self.test_entries:
            id, key, val, type_ = entry
            meta = service.create(run.id, key, val)

            assert meta.id == id
            assert meta.run__id == run.id
            assert meta.key == key
            assert meta.dtype == type_
            self.assert_value(meta, val)

    def test_meta_get(
        self,
        service: RunMetaEntryService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        for entry in self.test_entries:
            id, key, val, type_ = entry
            meta = service.get(run.id, key)

            assert meta.id == id
            assert meta.run__id == run.id
            assert meta.key == key
            self.assert_value(meta, val)


class TestRunMetaEntryDeleteById(RunMetaEntryServiceTest):
    def test_meta_delete_by_id(
        self,
        service: RunMetaEntryService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        for entry in self.test_entries:
            id, key, val, type_ = entry
            service.create(run.id, key, val)

        for entry in self.test_entries:
            id, key, val, type_ = entry
            service.delete_by_id(id)

            with pytest.raises(RunMetaEntryNotFound):
                service.get(run.id, key)

        assert service.tabulate().empty


class TestRunMetaEntryNotFound(RunMetaEntryServiceTest):
    def test_meta_not_found(
        self, runs: RunService, service: RunMetaEntryService
    ) -> None:
        with pytest.raises(RunMetaEntryNotFound):
            service.get(1, "dne")

        run = runs.create("Model", "Scenario")
        assert run.id == 1

        with pytest.raises(RunMetaEntryNotFound):
            service.get(1, "dne")


class TestRunMetaEntryList(RunMetaEntryServiceTest):
    def test_meta_list(
        self,
        service: RunMetaEntryService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        for entry in self.test_entries:
            id, key, val, type_ = entry
            service.create(run.id, key, val)

        metas = service.list()

        assert metas[0].id == 1
        assert metas[0].run__id == 1
        assert metas[0].key == "Boolean"
        self.assert_value(metas[0], True)

        assert metas[1].id == 2
        assert metas[1].run__id == 1
        assert metas[1].key == "Float"
        self.assert_value(metas[1], 0.2)

        assert metas[2].id == 3
        assert metas[2].run__id == 1
        assert metas[2].key == "Integer"
        self.assert_value(metas[2], 1)

        assert metas[3].id == 4
        assert metas[3].run__id == 1
        assert metas[3].key == "String"
        self.assert_value(metas[3], "Value")


class TestRunMetaEntryTabulate(RunMetaEntryServiceTest):
    def test_meta_tabulate(
        self,
        service: RunMetaEntryService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        for entry in self.test_entries:
            id, key, val, type_ = entry
            service.create(run.id, key, val)

        expected_metas = self.test_entries_df
        expected_metas["run__id"] = run.id

        metas = service.tabulate()
        pdt.assert_frame_equal(metas, expected_metas, check_like=True)


class TestRunMetaEntryBulkOperations(RunMetaEntryServiceTest):
    def test_meta_bulk_operations(
        self,
        service: RunMetaEntryService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        entries = self.test_entries_df
        entries["run__id"] = run.id

        # == Full Addition ==
        service.bulk_upsert(entries.drop(columns=["id", "dtype"]))
        ret = service.tabulate()
        pdt.assert_frame_equal(entries, ret)

        # == Partial Removal ==
        # Remove half the data
        remove_data = entries.head(len(entries) // 2).drop(
            columns=["value", "id", "dtype"]
        )
        remaining_data = entries.tail(len(entries) // 2).reset_index(drop=True)
        service.bulk_delete(remove_data)

        ret = service.tabulate()
        pdt.assert_frame_equal(remaining_data, ret)

        # == Partial Update / Partial Addition ==
        entries["value"] = -9.9
        entries["id"] = [5, 6, 3, 4]

        updated_entries = pd.DataFrame(
            [
                [3, "Integer", -9.9, Type.FLOAT],
                [4, "String", -9.9, Type.FLOAT],
                [5, "Boolean", -9.9, Type.FLOAT],
                [6, "Float", -9.9, Type.FLOAT],
            ],
            columns=["id", "key", "value", "dtype"],
        )
        updated_entries["run__id"] = run.id

        service.bulk_upsert(updated_entries.drop(columns=["id", "dtype"]))
        ret = service.tabulate()

        pdt.assert_frame_equal(updated_entries, ret, check_like=True)

        # == Full Removal ==
        remove_data = entries.drop(columns=["value", "id", "dtype"])
        service.bulk_delete(remove_data)

        ret = service.tabulate()
        assert ret.empty
