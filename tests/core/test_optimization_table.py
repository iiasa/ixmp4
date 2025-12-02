import datetime
from typing import Any

import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import OptimizationItemUsageError
from tests import backends

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class OptimizationTableTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def indexset(self, run: ixmp4.Run) -> ixmp4.optimization.IndexSet:
        with run.transact("Create indexset"):
            indexset = run.optimization.indexsets.create("IndexSet")
            indexset.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            return indexset


class TestTable(OptimizationTableTest):
    def test_create_table(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ):
        with run.transact("Create tables"):
            table1 = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=["IndexSet"]
            )

            table2 = run.optimization.tables.create(
                "Table 2", constrained_to_indexsets=["IndexSet"]
            )
            table3 = run.optimization.tables.create(
                "Table 3",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column"],
            )

            table4 = run.optimization.tables.create(
                "Table 4",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        assert table1.id == 1
        assert table1.run_id == run.id
        assert table1.name == "Table 1"
        assert table1.data == {}
        assert table1.indexset_names == ["IndexSet"]
        assert table1.column_names is None
        assert table1.created_by == "@unknown"
        assert table1.created_at == fake_time.replace(tzinfo=None)

        assert table2.id == 2
        assert table2.indexset_names == ["IndexSet"]
        assert table2.column_names is None

        assert table3.id == 3
        assert table3.indexset_names == ["IndexSet"]
        assert table3.column_names == ["Column"]

        assert table4.id == 4
        assert table4.indexset_names == ["IndexSet", "IndexSet"]
        assert table4.column_names == ["Column 1", "Column 2"]

    def test_tabulate_table(self, run: ixmp4.Run):
        ret_df = run.optimization.tables.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "data" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

        assert "run__id" not in ret_df.columns

    def test_list_table(self, run: ixmp4.Run):
        assert len(run.optimization.tables.list()) == 4

    def test_delete_table_via_func_obj(self, run: ixmp4.Run):
        with run.transact("Delete table 1"):
            table1 = run.optimization.tables.get_by_name("Table 1")
            run.optimization.tables.delete(table1)

    def test_delete_table_via_func_id(self, run: ixmp4.Run):
        with run.transact("Delete table 2"):
            run.optimization.tables.delete(2)

    def test_delete_table_via_func_name(self, run: ixmp4.Run):
        with run.transact("Delete table 3"):
            run.optimization.tables.delete("Table 3")

    def test_delete_table_via_obj(self, run: ixmp4.Run):
        table4 = run.optimization.tables.get_by_name("Table 4")
        with run.transact("Delete table 4"):
            table4.delete()

    def test_table_empty(self, run: ixmp4.Run):
        assert run.optimization.tables.tabulate().empty
        assert len(run.optimization.tables.list()) == 0


class TestTableUnique(OptimizationTableTest):
    def test_table_unique(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
    ) -> None:
        with run.transact("Table not unique"):
            run.optimization.tables.create(
                "Table", constrained_to_indexsets=[indexset.name]
            )

            with pytest.raises(ixmp4.optimization.Table.NotUnique):
                run.optimization.tables.create(
                    "Table", constrained_to_indexsets=[indexset.name]
                )


class TestTableNotFound(OptimizationTableTest):
    def test_table_not_found(
        self,
        run: ixmp4.Run,
    ) -> None:
        with pytest.raises(ixmp4.optimization.Table.NotFound):
            run.optimization.tables.get_by_name("Table")


class TestTableCreateInvalidArguments(OptimizationTableTest):
    def test_table_create_invalid_args(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Invalid arguments"):
            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                run.optimization.tables.create(
                    "Table",
                    constrained_to_indexsets=[indexset.name],
                    column_names=["Column 1", "Column 2"],
                )

            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                run.optimization.tables.create(
                    "Table",
                    constrained_to_indexsets=[indexset.name, indexset.name],
                    column_names=["Column 1", "Column 1"],
                )


class TableDataTest(OptimizationTableTest):
    @pytest.fixture(scope="class")
    def test_data_indexsets(self, run: ixmp4.Run) -> list[ixmp4.optimization.IndexSet]:
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        return [indexset1, indexset2]

    def test_table_add_data(
        self,
        run: ixmp4.Run,
        test_data_indexsets: list[ixmp4.optimization.IndexSet],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create table and add data"):
            table = run.optimization.tables.create(
                "Table",
                constrained_to_indexsets=[i.name for i in test_data_indexsets],
                column_names=column_names,
            )
            table.add_data(test_data)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert table.data == test_data

    def test_table_remove_data_partial(
        self,
        run: ixmp4.Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove table data partial"):
            table = run.optimization.tables.get_by_name("Table")
            table.remove_data(partial_test_data)

        assert table.data == remaining_test_data

    def test_table_remove_data_full(
        self,
        run: ixmp4.Run,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove table data full"):
            table = run.optimization.tables.get_by_name("Table")
            table.remove_data()

        assert table.data == {}


class TestTableData(TableDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def partial_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "IndexSet 1": ["re", "mi"],
            "IndexSet 2": [3, 1],
        }

    @pytest.fixture(scope="class")
    def remaining_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestTableDataWithColumnNames(TableDataTest):
    @pytest.fixture(scope="class")
    def column_names(self) -> list[str] | None:
        return ["Column 1", "Column 2"]

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "Column 1": ["do", "re", "mi"],
            "Column 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def partial_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "Column 1": ["re", "mi"],
            "Column 2": [3, 1],
        }

    @pytest.fixture(scope="class")
    def remaining_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "Column 1": ["do"],
            "Column 2": [3],
        }


class TestTableDataDataFrame(TableDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return pd.DataFrame(
            [
                ["do", 3],
                ["re", 3],
                ["mi", 1],
            ],
            columns=["IndexSet 1", "IndexSet 2"],
        )

    @pytest.fixture(scope="class")
    def partial_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return pd.DataFrame(
            [
                ["re", 3],
                ["mi", 1],
            ],
            columns=["IndexSet 1", "IndexSet 2"],
        )

    @pytest.fixture(scope="class")
    def remaining_test_data(self) -> dict[str, list[Any]]:
        return {
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestTableInvalidData(OptimizationTableTest):
    def test_tables_create(self, run: ixmp4.Run):
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        with run.transact("Create tables"):
            table1 = run.optimization.tables.create(
                "Table 1",
                constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
            )
            assert table1.id == 1

    def test_table_add_invalid_data(self, run: ixmp4.Run) -> None:
        table1 = run.optimization.tables.get_by_name("Table 1")

        with pytest.raises(
            ixmp4.optimization.Table.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                table1.add_data(
                    {
                        "IndexSet 1": ["do", "re"],  # missing "mi"
                        "IndexSet 2": [3, 3, 1],
                    }
                )

        with pytest.raises(
            ixmp4.optimization.Table.DataInvalid, match="contains duplicate rows"
        ):
            with run.transact("Add invalid data"):
                table1.add_data(
                    {
                        "IndexSet 1": ["do", "do", "mi"],
                        "IndexSet 2": [3, 3, 1],
                    },
                )

    def test_table_remove_invalid_data(self, run: ixmp4.Run) -> None:
        table1 = run.optimization.tables.get_by_name("Table 1")

        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                table1.remove_data({"IndexSet 1": ["do"]})


class TestTableRunLock(OptimizationTableTest):
    def test_table_requires_lock(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.tables.create(
                "Table", constrained_to_indexsets=[indexset.name]
            )

        with run.transact("Create table"):
            table = run.optimization.tables.create(
                "Table", constrained_to_indexsets=[indexset.name]
            )

        with pytest.raises(ixmp4.Run.LockRequired):
            table.add_data({"marginals": [1], "levels": [2], "IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            table.remove_data({"IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            table.delete()

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.tables.delete(table.id)


class TestTableDocs(OptimizationTableTest):
    def test_create_docs_via_func(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Create table 1"):
            table1 = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=[indexset.name]
            )

        table1_docs1 = run.optimization.tables.set_docs(
            "Table 1", "Description of Table 1"
        )
        table1_docs2 = run.optimization.tables.get_docs("Table 1")

        assert table1_docs1 == table1_docs2
        assert table1.docs == table1_docs1

    def test_create_docs_via_object(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Create table 2"):
            table2 = run.optimization.tables.create(
                "Table 2", constrained_to_indexsets=[indexset.name]
            )
        table2.docs = "Description of Table 2"

        assert run.optimization.tables.get_docs("Table 2") == table2.docs

    def test_create_docs_via_setattr(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Create table 3"):
            table3 = run.optimization.tables.create(
                "Table 3", constrained_to_indexsets=[indexset.name]
            )
        setattr(table3, "docs", "Description of Table 3")

        assert run.optimization.tables.get_docs("Table 3") == table3.docs

    def test_list_docs(self, run: ixmp4.Run) -> None:
        assert run.optimization.tables.list_docs() == [
            "Description of Table 1",
            "Description of Table 2",
            "Description of Table 3",
        ]

        assert run.optimization.tables.list_docs(id=3) == ["Description of Table 3"]

        assert run.optimization.tables.list_docs(id__in=[1]) == [
            "Description of Table 1"
        ]

    def test_delete_docs_via_func(self, run: ixmp4.Run) -> None:
        table1 = run.optimization.tables.get_by_name("Table 1")
        run.optimization.tables.delete_docs("Table 1")
        table1 = run.optimization.tables.get_by_name("Table 1")
        assert table1.docs is None

    def test_delete_docs_set_none(self, run: ixmp4.Run) -> None:
        table2 = run.optimization.tables.get_by_name("Table 2")
        table2.docs = None
        table2 = run.optimization.tables.get_by_name("Table 2")
        assert table2.docs is None

    def test_delete_docs_del(self, run: ixmp4.Run) -> None:
        table3 = run.optimization.tables.get_by_name("Table 3")
        del table3.docs
        table3 = run.optimization.tables.get_by_name("Table 3")
        assert table3.docs is None

    def test_docs_empty(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.tables.list_docs()) == 0


class TestTableRollback(OptimizationTableTest):
    def test_table_add_data_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ):
        with run.transact("Add table data"):
            table = run.optimization.tables.create(
                "Table", constrained_to_indexsets=[indexset.name]
            )
            table.add_data(
                {
                    "IndexSet": ["do", "re", "mi"],
                }
            )

        try:
            with run.transact("Update table data failure"):
                table.add_data(
                    {
                        "IndexSet": ["fa", "so"],
                    }
                )
                raise Exception("Whoops!!!")
        except Exception:
            pass

    def test_table_versioning_after_add_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.data == {
            "IndexSet": ["do", "re", "mi"],
        }

    def test_table_non_versioning_after_add_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.data == {
            "IndexSet": ["do", "fa", "mi", "re", "so"],
        }

    def test_table_remove_data_failure(self, run: ixmp4.Run):
        table = run.optimization.tables.get_by_name("Table")

        with run.transact("Remove table data"):
            table.remove_data(
                {
                    "IndexSet": ["fa", "so"],
                }
            )

        try:
            with run.transact("Remove table data failure"):
                table.remove_data({"IndexSet": ["do", "re"]})
                raise Exception("Whoops!!!")
        except Exception:
            pass

    def test_table_versioning_after_remove_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.data == {
            "IndexSet": ["do", "re", "mi"],
        }

    def test_table_non_versioning_after_remove_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.data == {
            "IndexSet": ["mi"],
        }

    def test_table_docs_failure(self, run: ixmp4.Run):
        table = run.optimization.tables.get_by_name("Table")
        table.docs = "These docs should persist!"

        try:
            with run.transact("Set table docs failure"):
                table.docs = "These docs should be rolled back!"
                raise Exception("Whoops!!!")
        except Exception:
            pass

    def test_table_versioning_after_docs_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.docs == "These docs should persist!"

    def test_table_non_versioning_after_docs_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.docs == "These docs should be rolled back!"

    def test_table_delete_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ):
        table = run.optimization.tables.get_by_name("Table")
        table.docs = "These docs should persist!"

        try:
            with run.transact("Delete table failure"):
                table.delete()
                indexset.delete()
                raise Exception("Whoops!!!")
        except Exception:
            pass

    def test_table_versioning_after_delete_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        table = run.optimization.tables.get_by_name("Table")
        assert table.id == 2

    def test_table_non_versioning_after_delete_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        with pytest.raises(ixmp4.optimization.Table.NotFound):
            run.optimization.tables.get_by_name("Table")
