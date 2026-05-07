import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden, InvalidDataFrame
from ixmp4.data.meta.dto import MetaValueType, RunMetaEntry
from ixmp4.data.meta.exceptions import RunMetaEntryNotFound
from ixmp4.data.meta.service import RunMetaEntryService
from ixmp4.data.meta.type import Type
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import Transport
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


# TODO: Versions
class RunMetaEntryServiceTest(ServiceTest[RunMetaEntryService]):
    service_class = RunMetaEntryService

    @pytest.fixture(scope="class")
    def test_entries(self) -> list[tuple[int, str, MetaValueType, Type]]:
        return [
            (1, "Boolean", True, Type.BOOL),
            (2, "Float", 0.2, Type.FLOAT),
            (3, "Integer", 1, Type.INT),
            (4, "String", "Value", Type.STR),
        ]

    @pytest.fixture(scope="class")
    def test_entries_df(
        self, test_entries: list[tuple[int, str, MetaValueType, Type]], run: Run
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [id, run.id, key, val, str(type_)]
                for id, key, val, type_ in test_entries
            ],
            columns=["id", "run__id", "key", "value", "dtype"],
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
        runs.set_as_default_version(run.id)
        assert run.id == 1
        return run

    def assert_value(self, meta: RunMetaEntry, val: MetaValueType) -> None:
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
        test_entries: list[tuple[int, str, MetaValueType, Type]],
        fake_time: datetime.datetime,
    ) -> None:
        for entry in test_entries:
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
        test_entries: list[tuple[int, str, MetaValueType, Type]],
        fake_time: datetime.datetime,
    ) -> None:
        for entry in test_entries:
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
        test_entries: list[tuple[int, str, MetaValueType, Type]],
        fake_time: datetime.datetime,
    ) -> None:
        for entry in test_entries:
            id, key, val, type_ = entry
            service.create(run.id, key, val)

        for entry in test_entries:
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
        test_entries: list[tuple[int, str, MetaValueType, Type]],
        fake_time: datetime.datetime,
    ) -> None:
        for entry in test_entries:
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
        test_entries: list[tuple[int, str, MetaValueType, Type]],
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        for entry in test_entries:
            id, key, val, type_ = entry
            service.create(run.id, key, val)

        metas = service.tabulate()
        pdt.assert_frame_equal(metas, test_entries_df, check_like=True)


class TestRunMetaEntryBulkOperations(RunMetaEntryServiceTest):
    def test_meta_bulk_insert(
        self,
        service: RunMetaEntryService,
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        # == Full Addition ==
        service.bulk_upsert(test_entries_df.drop(columns=["id", "dtype"]))
        ret = service.tabulate()
        pdt.assert_frame_equal(
            self.canonical_sort(test_entries_df),
            self.canonical_sort(ret),
            check_like=True,
        )

    def test_meta_bulk_delete_half(
        self,
        service: RunMetaEntryService,
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        remove_data = test_entries_df.head(len(test_entries_df) // 2).drop(
            columns=["value", "id", "dtype"]
        )
        remaining_data = test_entries_df.tail(len(test_entries_df) // 2).reset_index(
            drop=True
        )
        service.bulk_delete(remove_data)

        ret = service.tabulate()
        pdt.assert_frame_equal(remaining_data, ret, check_like=True)

    def test_meta_bulk_upsert(
        self,
        service: RunMetaEntryService,
        run: Run,
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        test_entries_df["value"] = -9.9
        test_entries_df["id"] = [5, 6, 3, 4]

        updated_entries = pd.DataFrame(
            [
                [5, "Boolean", -9.9, Type.FLOAT],
                [6, "Float", -9.9, Type.FLOAT],
                [3, "Integer", -9.9, Type.FLOAT],
                [4, "String", -9.9, Type.FLOAT],
            ],
            columns=["id", "key", "value", "dtype"],
        )
        updated_entries["run__id"] = run.id

        service.bulk_upsert(updated_entries.drop(columns=["id", "dtype"]))
        ret = service.tabulate()

        pdt.assert_frame_equal(updated_entries, ret, check_like=True)

    def test_meta_bulk_delete_full(
        self,
        service: RunMetaEntryService,
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        remove_data = test_entries_df.drop(columns=["value", "id", "dtype"])
        service.bulk_delete(remove_data)

        ret = service.tabulate()
        assert ret.empty

    def test_meta_bulk_upsert_invalid(
        self,
        service: RunMetaEntryService,
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        invalid_upsert_data = test_entries_df.drop(
            columns=["key", "value", "id", "dtype"]
        )  # no `run__id`

        with pytest.raises(InvalidDataFrame):
            service.bulk_upsert(invalid_upsert_data)

    def test_meta_bulk_delete_invalid(
        self,
        service: RunMetaEntryService,
        test_entries_df: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        invalid_remove_data = test_entries_df.drop(
            columns=["run__id", "value", "id", "dtype"]
        )  # no `key`

        with pytest.raises(InvalidDataFrame):
            service.bulk_delete(invalid_remove_data)


class RunMetaEntryAuthTest(RunMetaEntryServiceTest):
    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return RunService(direct)

    @pytest.fixture(scope="class")
    def run(self, runs: RunService) -> Run:
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        return run

    @pytest.fixture(scope="class")
    def run1(self, runs: RunService) -> Run:
        run = runs.create("Model 1", "Scenario")
        runs.set_as_default_version(run.id)
        return run

    @pytest.fixture(scope="class")
    def run2(self, runs: RunService) -> Run:
        run = runs.create("Model 2", "Scenario")
        runs.set_as_default_version(run.id)
        return run

    @pytest.fixture(scope="class")
    def test_df(self, run: Run) -> pd.DataFrame:
        return pd.DataFrame(
            [[run.id, "Meta", 1.0]], columns=["run__id", "key", "value"]
        )

    @pytest.fixture(scope="class")
    def test_df1(self, run1: Run) -> pd.DataFrame:
        return pd.DataFrame(
            [[run1.id, "Meta 1", 2.0]], columns=["run__id", "key", "value"]
        )

    @pytest.fixture(scope="class")
    def test_df2(self, run2: Run) -> pd.DataFrame:
        return pd.DataFrame(
            [[run2.id, "Meta 2", 3.0]], columns=["run__id", "key", "value"]
        )


class TestRunMetaEntryAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, RunMetaEntryAuthTest
):
    def test_meta_create(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.create(run.id, "Meta", 1.0)
        assert meta.id == 1

    def test_meta_get(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.get(run.id, "Meta")
        assert meta.id == 1

    def test_meta_list(self, service: RunMetaEntryService) -> None:
        metas = service.list()
        assert len(metas) == 1

    def test_meta_tabulate(self, service: RunMetaEntryService) -> None:
        metas = service.tabulate()
        assert len(metas) == 1

    def test_meta_bulk_upsert(
        self, service: RunMetaEntryService, test_df: pd.DataFrame
    ) -> None:
        service.bulk_upsert(test_df)

    def test_meta_bulk_delete(
        self, service: RunMetaEntryService, test_df: pd.DataFrame
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        service.bulk_delete(delete_df)

    def test_meta_delete_by_id(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.create(run.id, "Delete Meta", 4.0)
        service.delete_by_id(meta.id)


class TestRunMetaEntryAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, RunMetaEntryAuthTest
):
    def test_meta_create(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        run: Run,
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(run.id, "Meta", 1.0)

        meta = unauthorized_service.create(run.id, "Meta", 1.0)
        assert meta.id == 1

    def test_meta_get(self, service: RunMetaEntryService, run: Run) -> None:
        with pytest.raises(Forbidden):
            service.get(run.id, "Meta")

    def test_meta_list(self, service: RunMetaEntryService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_meta_tabulate(self, service: RunMetaEntryService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_meta_bulk_upsert(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        test_df: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df)

        unauthorized_service.bulk_upsert(test_df)

    def test_meta_bulk_delete(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        test_df: pd.DataFrame,
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df)

        unauthorized_service.bulk_upsert(test_df)

    def test_meta_delete_by_id(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        run: Run,
    ) -> None:
        meta = unauthorized_service.create(run.id, "Delete Meta", 4.0)
        with pytest.raises(RunMetaEntryNotFound):
            service.delete_by_id(meta.id)


class TestRunMetaEntryAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, RunMetaEntryAuthTest
):
    def test_meta_create(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.create(run.id, "Meta", 1.0)
        assert meta.id == 1

    def test_meta_get(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.get(run.id, "Meta")
        assert meta.id == 1

    def test_meta_list(self, service: RunMetaEntryService) -> None:
        metas = service.list()
        assert len(metas) == 1

    def test_meta_tabulate(self, service: RunMetaEntryService) -> None:
        metas = service.tabulate()
        assert len(metas) == 1

    def test_meta_bulk_upsert(
        self, service: RunMetaEntryService, test_df: pd.DataFrame
    ) -> None:
        service.bulk_upsert(test_df)

    def test_meta_bulk_delete(
        self, service: RunMetaEntryService, test_df: pd.DataFrame
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        service.bulk_delete(delete_df)

    def test_meta_delete_by_id(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.create(run.id, "Delete Meta", 4.0)
        service.delete_by_id(meta.id)


class TestRunMetaEntryAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, RunMetaEntryAuthTest
):
    def test_meta_create(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        run: Run,
        run1: Run,
        run2: Run,
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(run.id, "Meta", 1.0)
        meta = unauthorized_service.create(run.id, "Meta", 1.0)
        assert meta.id == 1

        meta1 = service.create(run1.id, "Meta 1", 2.0)
        assert meta1.id == 2

        with pytest.raises(Forbidden):
            service.create(run2.id, "Meta 2", 3.0)
        meta2 = unauthorized_service.create(run2.id, "Meta 2", 3.0)
        assert meta2.id == 3

    def test_meta_get(
        self, service: RunMetaEntryService, run: Run, run1: Run, run2: Run
    ) -> None:
        meta = service.get(run.id, "Meta")
        assert meta.id == 1

        meta1 = service.get(run1.id, "Meta 1")
        assert meta1.id == 2

        with pytest.raises(RunMetaEntryNotFound):
            service.get(run2.id, "Meta 2")

    def test_meta_list(self, service: RunMetaEntryService) -> None:
        metas = service.list(run={"default_only": False})
        assert len(metas) == 2

    def test_meta_tabulate(self, service: RunMetaEntryService) -> None:
        metas = service.tabulate(run={"default_only": False})
        assert len(metas) == 2

    def test_meta_bulk_upsert(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        test_df: pd.DataFrame,
        test_df1: pd.DataFrame,
        test_df2: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df)
        unauthorized_service.bulk_upsert(test_df)

        service.bulk_upsert(test_df1)

        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df2)
        unauthorized_service.bulk_upsert(test_df2)

    def test_meta_bulk_delete(
        self,
        service: RunMetaEntryService,
        test_df: pd.DataFrame,
        test_df1: pd.DataFrame,
        test_df2: pd.DataFrame,
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df)

        delete_df1 = test_df1.drop(columns=["value"])
        service.bulk_delete(delete_df1)

        delete_df2 = test_df2.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df2)

    def test_meta_delete_by_id(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        run: Run,
        run1: Run,
        run2: Run,
    ) -> None:
        meta = unauthorized_service.create(run.id, "Delete Meta", 4.0)
        meta1 = service.create(run1.id, "Delete Meta 1", 5.0)
        meta2 = unauthorized_service.create(run2.id, "Delete Meta 2", 6.0)

        with pytest.raises(Forbidden):
            service.delete_by_id(meta.id)

        service.delete_by_id(meta1.id)

        with pytest.raises(RunMetaEntryNotFound):
            service.delete_by_id(meta2.id)


class TestRunMetaEntryAuthNonePrivate(
    auth.NoneTest, auth.PrivatePlatformTest, RunMetaEntryAuthTest
):
    def test_meta_create(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        run: Run,
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(run.id, "Meta", 1.0)

        meta = unauthorized_service.create(run.id, "Meta", 1.0)
        assert meta.id == 1

    def test_meta_get(self, service: RunMetaEntryService, run: Run) -> None:
        with pytest.raises(Forbidden):
            service.get(run.id, "Meta")

    def test_meta_list(self, service: RunMetaEntryService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_meta_tabulate(self, service: RunMetaEntryService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_meta_bulk_upsert(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        test_df: pd.DataFrame,
    ) -> None:
        with pytest.raises(Forbidden):
            service.bulk_upsert(test_df)

        unauthorized_service.bulk_upsert(test_df)

    def test_meta_bulk_delete(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        test_df: pd.DataFrame,
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        with pytest.raises(Forbidden):
            service.bulk_delete(delete_df)

        unauthorized_service.bulk_upsert(test_df)

    def test_meta_delete_by_id(
        self,
        service: RunMetaEntryService,
        unauthorized_service: RunMetaEntryService,
        run: Run,
    ) -> None:
        meta = unauthorized_service.create(run.id, "Delete Meta", 4.0)
        with pytest.raises(RunMetaEntryNotFound):
            service.delete_by_id(meta.id)


class TestRunMetaEntryAuthDaveGated(
    auth.DaveTest, auth.GatedPlatformTest, RunMetaEntryAuthTest
):
    def test_meta_create(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.create(run.id, "Meta", 1.0)
        assert meta.id == 1

    def test_meta_get(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.get(run.id, "Meta")
        assert meta.id == 1

    def test_meta_list(self, service: RunMetaEntryService) -> None:
        metas = service.list()
        assert len(metas) == 1

    def test_meta_tabulate(self, service: RunMetaEntryService) -> None:
        metas = service.tabulate()
        assert len(metas) == 1

    def test_meta_bulk_upsert(
        self, service: RunMetaEntryService, test_df: pd.DataFrame
    ) -> None:
        service.bulk_upsert(test_df)

    def test_meta_bulk_delete(
        self, service: RunMetaEntryService, test_df: pd.DataFrame
    ) -> None:
        delete_df = test_df.drop(columns=["value"])
        service.bulk_delete(delete_df)

    def test_meta_delete_by_id(self, service: RunMetaEntryService, run: Run) -> None:
        meta = service.create(run.id, "Delete Meta", 4.0)
        service.delete_by_id(meta.id)
