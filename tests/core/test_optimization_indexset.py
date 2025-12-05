import datetime
from typing import Any

import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import InvalidArguments
from tests import backends
from tests.custom_exception import CustomException

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class OptimizationIndexSetTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1
        return run


class TestIndexSet(OptimizationIndexSetTest):
    def test_create_indexset(
        self,
        run: ixmp4.Run,
        fake_time: datetime.datetime,
    ):
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")

            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset3 = run.optimization.indexsets.create("IndexSet 3")

            indexset4 = run.optimization.indexsets.create("IndexSet 4")

        assert indexset1.id == 1
        assert indexset1.run_id == run.id
        assert indexset1.name == "IndexSet 1"
        assert indexset1.data == []
        assert indexset1.created_by == "@unknown"
        assert indexset1.created_at == fake_time.replace(tzinfo=None)

        assert indexset2.id == 2

        assert indexset3.id == 3

        assert indexset4.id == 4

    def test_tabulate_indexset(self, run: ixmp4.Run):
        ret_df = run.optimization.indexsets.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "data_type" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

        assert "run__id" not in ret_df.columns

    def test_list_indexset(self, run: ixmp4.Run):
        assert len(run.optimization.indexsets.list()) == 4

    def test_delete_indexset_via_func_obj(self, run: ixmp4.Run):
        with run.transact("Delete indexset 1"):
            indexset1 = run.optimization.indexsets.get_by_name("IndexSet 1")
            run.optimization.indexsets.delete(indexset1)

    def test_delete_indexset_via_func_id(self, run: ixmp4.Run):
        with run.transact("Delete indexset 2"):
            run.optimization.indexsets.delete(2)

    def test_delete_indexset_via_func_name(self, run: ixmp4.Run):
        with run.transact("Delete indexset 3"):
            run.optimization.indexsets.delete("IndexSet 3")

    def test_delete_indexset_via_obj(self, run: ixmp4.Run):
        indexset4 = run.optimization.indexsets.get_by_name("IndexSet 4")
        with run.transact("Delete indexset 4"):
            indexset4.delete()

    def test_indexset_empty(self, run: ixmp4.Run):
        assert run.optimization.indexsets.tabulate().empty
        assert len(run.optimization.indexsets.list()) == 0


class TestIndexSetUnique(OptimizationIndexSetTest):
    def test_indexset_unique(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("IndexSet not unique"):
            run.optimization.indexsets.create("IndexSet")

            with pytest.raises(ixmp4.optimization.IndexSet.NotUnique):
                run.optimization.indexsets.create("IndexSet")


class TestIndexSetNotFound(OptimizationIndexSetTest):
    def test_indexset_not_found(
        self,
        run: ixmp4.Run,
    ) -> None:
        with pytest.raises(ixmp4.optimization.IndexSet.NotFound):
            run.optimization.indexsets.get_by_name("IndexSet")


class IndexSetDataTest(OptimizationIndexSetTest):
    def test_indexset_add_data(
        self,
        run: ixmp4.Run,
        test_data: str | int | float | list[str] | list[int] | list[float],
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create indexset and add data"):
            indexset = run.optimization.indexsets.create("IndexSet")
            indexset.add_data(test_data)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        if isinstance(test_data, list):
            assert indexset.data == test_data
        else:
            assert indexset.data == [test_data]

    def test_indexset_remove_data_partial(
        self,
        run: ixmp4.Run,
        test_data_remove: str | int | float | list[str] | list[int] | list[float],
        test_data_remaining: str | int | float | list[str] | list[int] | list[float],
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove indexset data partial"):
            indexset = run.optimization.indexsets.get_by_name("IndexSet")
            indexset.remove_data(test_data_remove)

        assert indexset.data == test_data_remaining


class TestIndexSetDataStringList(IndexSetDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> str | int | float | list[str] | list[int] | list[float]:
        return ["do", "re", "mi", "fa", "so", "la", "ti"]

    @pytest.fixture(scope="class")
    def test_data_remove(self) -> dict[str, list[Any]] | pd.DataFrame:
        return ["do", "mi", "fa", "so"]

    @pytest.fixture(scope="class")
    def test_data_remaining(self) -> dict[str, list[Any]] | pd.DataFrame:
        return ["re", "la", "ti"]


class TestIndexSetAddInvalidData(OptimizationIndexSetTest):
    def test_indexset_add_invalid_data(self, run: ixmp4.Run):
        with run.transact("Add invalid data"):
            indexset = run.optimization.indexsets.create("IndexSet 1")
            assert indexset.id == 1

            with pytest.raises(
                (ixmp4.optimization.IndexSet.DataInvalid, InvalidArguments)
            ):
                indexset.add_data([True])

            with pytest.raises(
                (ixmp4.optimization.IndexSet.DataInvalid, InvalidArguments)
            ):
                indexset.add_data([datetime.datetime.now(), datetime.datetime.now()])


class TestIndexSetAppendInvalidData(OptimizationIndexSetTest):
    def test_indexset_append_invalid_data(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Add invalid data"):
            indexset = run.optimization.indexsets.create("IndexSet 1")
            assert indexset.id == 1
            indexset.add_data([1, 2, 3])

            with pytest.raises(ixmp4.optimization.IndexSet.DataInvalid):
                indexset.add_data(["one", "two", "three"])

            with pytest.raises(ixmp4.optimization.IndexSet.DataInvalid):
                indexset.add_data(3.142)


class TestIndexSetRunLock(OptimizationIndexSetTest):
    def test_indexset_requires_lock(self, run: ixmp4.Run) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.indexsets.create("IndexSet")

        with run.transact("Create indexset"):
            indexset = run.optimization.indexsets.create("IndexSet")

        with pytest.raises(ixmp4.Run.LockRequired):
            indexset.add_data(["foo", "bar", "baz"])

        with pytest.raises(ixmp4.Run.LockRequired):
            indexset.remove_data("foo")

        with pytest.raises(ixmp4.Run.LockRequired):
            indexset.delete()

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.indexsets.delete(indexset.id)


class TestIndexSetDocs(OptimizationIndexSetTest):
    def test_create_docs_via_func(self, run: ixmp4.Run) -> None:
        with run.transact("Create indexset 1"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")

        indexset1_docs1 = run.optimization.indexsets.set_docs(
            "IndexSet 1", "Description of IndexSet 1"
        )
        indexset1_docs2 = run.optimization.indexsets.get_docs("IndexSet 1")

        assert indexset1_docs1 == indexset1_docs2
        assert indexset1.docs == indexset1_docs1

    def test_create_docs_via_object(self, run: ixmp4.Run) -> None:
        with run.transact("Create indexset 2"):
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
        indexset2.docs = "Description of IndexSet 2"

        assert run.optimization.indexsets.get_docs("IndexSet 2") == indexset2.docs

    def test_create_docs_via_setattr(self, run: ixmp4.Run) -> None:
        with run.transact("Create indexset 3"):
            indexset3 = run.optimization.indexsets.create("IndexSet 3")
        setattr(indexset3, "docs", "Description of IndexSet 3")

        assert run.optimization.indexsets.get_docs("IndexSet 3") == indexset3.docs

    def test_list_docs(self, run: ixmp4.Run) -> None:
        assert run.optimization.indexsets.list_docs() == [
            "Description of IndexSet 1",
            "Description of IndexSet 2",
            "Description of IndexSet 3",
        ]

        assert run.optimization.indexsets.list_docs(id=3) == [
            "Description of IndexSet 3"
        ]

        assert run.optimization.indexsets.list_docs(id__in=[1]) == [
            "Description of IndexSet 1"
        ]

    def test_delete_docs_via_func(self, run: ixmp4.Run) -> None:
        indexset1 = run.optimization.indexsets.get_by_name("IndexSet 1")
        run.optimization.indexsets.delete_docs("IndexSet 1")
        indexset1 = run.optimization.indexsets.get_by_name("IndexSet 1")
        assert indexset1.docs is None

    def test_delete_docs_set_none(self, run: ixmp4.Run) -> None:
        indexset2 = run.optimization.indexsets.get_by_name("IndexSet 2")
        indexset2.docs = None
        indexset2 = run.optimization.indexsets.get_by_name("IndexSet 2")
        assert indexset2.docs is None

    def test_delete_docs_del(self, run: ixmp4.Run) -> None:
        indexset3 = run.optimization.indexsets.get_by_name("IndexSet 3")
        del indexset3.docs
        indexset3 = run.optimization.indexsets.get_by_name("IndexSet 3")
        assert indexset3.docs is None

    def test_docs_empty(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.indexsets.list_docs()) == 0


class TestIndexSetRollback(OptimizationIndexSetTest):
    def test_indexset_add_data_failure(
        self,
        run: ixmp4.Run,
    ):
        with run.transact("Add indexset data"):
            indexset = run.optimization.indexsets.create("IndexSet")
            indexset.add_data(["foo", "bar", "baz"])

        try:
            with run.transact("Update indexset data failure"):
                indexset.add_data(["one", "two", "three"])
                raise CustomException
        except CustomException:
            pass

    def test_indexset_versioning_after_add_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")
        assert indexset.data == ["foo", "bar", "baz"]

    def test_indexset_non_versioning_after_add_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")
        assert indexset.data == ["foo", "bar", "baz", "one", "two", "three"]

    def test_indexset_remove_data_failure(self, run: ixmp4.Run):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")

        with run.transact("Remove indexset data"):
            indexset.remove_data(["one", "two", "three"])

        try:
            with run.transact("Remove indexset data failure"):
                indexset.remove_data("foo")
                raise CustomException
        except CustomException:
            pass

    def test_indexset_versioning_after_remove_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")
        assert indexset.data == ["foo", "bar", "baz"]

    def test_indexset_non_versioning_after_remove_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")
        assert indexset.data == ["bar", "baz"]

    def test_indexset_docs_failure(self, run: ixmp4.Run):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")

        try:
            with run.transact("Set indexset docs failure"):
                indexset.docs = "These docs should persist!"
                raise CustomException
        except CustomException:
            pass

    def test_indexset_after_docs_failure(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")
        assert indexset.docs == "These docs should persist!"

    def test_indexset_delete_failure(self, run: ixmp4.Run):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")

        try:
            with run.transact("Delete indexset failure"):
                indexset.delete()
                raise CustomException
        except CustomException:
            pass

    def test_indexset_versioning_after_delete_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        indexset = run.optimization.indexsets.get_by_name("IndexSet")
        assert indexset.id == 1

    def test_indexset_non_versioning_after_delete_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        with pytest.raises(ixmp4.optimization.IndexSet.NotFound):
            run.optimization.indexsets.get_by_name("IndexSet")
