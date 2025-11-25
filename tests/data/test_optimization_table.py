import datetime
from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.optimization.indexset.service import IndexSet, IndexSetService
from ixmp4.data.optimization.table.repositories import (
    TableDataInvalid,
    TableNotFound,
    TableNotUnique,
)
from ixmp4.data.optimization.table.service import TableService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.exceptions import OptimizationItemUsageError
from ixmp4.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class TableServiceTest(ServiceTest[TableService]):
    service_class = TableService

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

    @pytest.fixture(scope="class")
    def indexsets(self, transport: Transport) -> IndexSetService:
        return IndexSetService(transport)

    @pytest.fixture(scope="class")
    def indexset(self, run: Run, indexsets: IndexSetService) -> IndexSet:
        return indexsets.create(run.id, "IndexSet")


class TestTableCreate(TableServiceTest):
    def test_table_create(
        self,
        service: TableService,
        run: Run,
        fake_time: datetime.datetime,
        indexset: IndexSet,
    ) -> None:
        table = service.create(
            run.id,
            "Table",
            constrained_to_indexsets=["IndexSet"],
            column_names=["Column"],
        )
        assert table.run__id == run.id
        assert table.name == "Table"
        assert table.data == {}
        assert table.indexset_names == ["IndexSet"]
        assert table.column_names == ["Column"]

        assert table.created_at == fake_time.replace(tzinfo=None)
        assert table.created_by == "@unknown"

    def test_table_create_versioning(
        self,
        versioning_service: TableService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Table",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    5,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "data",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestTableCreateInvalidArguments(TableServiceTest):
    def test_table_create_invalid_args(
        self,
        service: TableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexsets.create(run.id, "IndexSet")
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            service.create(
                run.id,
                "Table",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            service.create(
                run.id,
                "Table",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 1"],
            )


class TestTableDeleteById(TableServiceTest):
    def test_table_delete_by_id(
        self,
        service: TableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexsets.create(run.id, "IndexSet")
        table = service.create(
            run.id,
            "Table",
            constrained_to_indexsets=["IndexSet"],
            column_names=["Column"],
        )
        service.delete_by_id(table.id)
        assert service.tabulate().empty

    def test_table_delete_by_id_versioning(
        self,
        versioning_service: TableService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Table",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    5,
                    8,
                    0,
                ],
                [
                    1,
                    run.id,
                    "Table",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    8,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "data",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )
        # TODO Association Versions


class TestTableUnique(TableServiceTest):
    def test_table_unique(
        self, service: TableService, run: Run, indexset: IndexSet
    ) -> None:
        service.create(run.id, "Table", constrained_to_indexsets=["IndexSet"])

        with pytest.raises(TableNotUnique):
            service.create(
                run.id,
                "Table",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 2"],
            )


class TestTableGetByName(TableServiceTest):
    def test_table_get(
        self, service: TableService, run: Run, indexset: IndexSet
    ) -> None:
        table1 = service.create(run.id, "Table", constrained_to_indexsets=["IndexSet"])
        table2 = service.get(run.id, "Table")
        assert table1 == table2


class TestTableGetById(TableServiceTest):
    def test_table_get_by_id(
        self, service: TableService, run: Run, indexset: IndexSet
    ) -> None:
        table1 = service.create(run.id, "Table", constrained_to_indexsets=["IndexSet"])
        table2 = service.get_by_id(1)
        assert table1 == table2


class TestTableNotFound(TableServiceTest):
    def test_table_not_found(self, service: TableService, run: Run) -> None:
        with pytest.raises(TableNotFound):
            service.get(run.id, "Table")

        with pytest.raises(TableNotFound):
            service.get_by_id(1)


class TableDataTest(TableServiceTest):
    def test_table_add_data(
        self,
        service: TableService,
        run: Run,
        test_data_indexsets: list[IndexSet],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        table = service.create(
            run.id,
            "Table",
            constrained_to_indexsets=[i.name for i in test_data_indexsets],
            column_names=column_names,
        )
        service.add_data(table.id, test_data)
        table = service.get_by_id(table.id)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert table.data == test_data

    def test_table_remove_data_partial(
        self,
        service: TableService,
        run: Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        table = service.get(run.id, "Table")
        service.remove_data(table.id, partial_test_data)
        table = service.get_by_id(table.id)
        assert table.data == remaining_test_data

    def test_table_remove_data_all(
        self,
        service: TableService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        table = service.get(run.id, "Table")
        service.remove_data(table.id)
        table = service.get_by_id(table.id)
        assert table.data == {}

    def test_table_data_versioning(
        self,
        versioning_service: IndexSetService,
        run: Run,
        test_data_indexsets: list[IndexSet],
        test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        if isinstance(remaining_test_data, pd.DataFrame):
            remaining_test_data = remaining_test_data.to_dict(orient="list")

        # compute transaction ids
        is_tx = (
            5 + len(test_data_indexsets) + sum(len(i.data) for i in test_data_indexsets)
        )
        create_tx = is_tx + 1
        add_data_tx = create_tx + 3
        rm_data_partial_tx = add_data_tx + 1
        rm_data_full_tx = rm_data_partial_tx + 1

        expected_versions = pd.DataFrame(
            [
                [
                    {},
                    create_tx,
                    add_data_tx,
                    0,
                ],
                [
                    test_data,
                    add_data_tx,
                    rm_data_partial_tx,
                    1,
                ],
                [
                    remaining_test_data,
                    rm_data_partial_tx,
                    rm_data_full_tx,
                    1,
                ],
                [
                    {},
                    rm_data_full_tx,
                    None,
                    1,
                ],
            ],
            columns=[
                "data",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        expected_versions["id"] = 1
        expected_versions["run__id"] = run.id
        expected_versions["name"] = "Table"
        expected_versions["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_versions["created_by"] = "@unknown"

        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestTableData(TableDataTest):
    @pytest.fixture(scope="class")
    def test_data_indexsets(
        self, run: Run, indexsets: IndexSetService
    ) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

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
    def test_data_indexsets(
        self, run: Run, indexsets: IndexSetService
    ) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
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
    def test_data_indexsets(
        self, run: Run, indexsets: IndexSetService
    ) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

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


class TestTableInvalidData(TableServiceTest):
    def test_tables_create(
        self, service: TableService, indexsets: IndexSetService, run: Run
    ):
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        table = service.create(
            run.id,
            "Table 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert table.id == 1

    def test_table_add_invalid_data(
        self,
        service: TableService,
    ) -> None:
        with pytest.raises(
            TableDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "IndexSet 1": ["do", "re"],  # missing "mi"
                    "IndexSet 2": [3, 3, 1],
                },
            )
        with pytest.raises(TableDataInvalid, match="contains duplicate rows"):
            service.add_data(
                1,
                {
                    "IndexSet 1": ["do", "do", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

    def test_table_remove_invalid_data(
        self,
        service: TableService,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            service.remove_data(
                1,
                {
                    "IndexSet 1": ["do"],
                },
            )


class TestTableList(TableServiceTest):
    def test_table_list(
        self,
        service: TableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Table 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(
            run.id,
            "Table 2",
            constrained_to_indexsets=["IndexSet 1"],
            column_names=["Column 1"],
        )

        test_data1 = {
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        tables = service.list()

        assert tables[0].id == 1
        assert tables[0].run__id == run.id
        assert tables[0].name == "Table 1"
        assert tables[0].data == test_data1
        assert tables[0].created_by == "@unknown"
        assert tables[0].created_at == fake_time.replace(tzinfo=None)

        assert tables[1].id == 2
        assert tables[1].run__id == run.id
        assert tables[1].name == "Table 2"
        assert tables[1].data == {}
        assert tables[1].created_by == "@unknown"
        assert tables[1].created_at == fake_time.replace(tzinfo=None)


class TestTableTabulate(TableServiceTest):
    def test_table_tabulate(
        self,
        service: TableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Table 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(
            run.id,
            "Table 2",
            constrained_to_indexsets=["IndexSet 1"],
            column_names=["Column 1"],
        )

        test_data1 = {
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        expected_tables = pd.DataFrame(
            [
                [1, "Table 1", test_data1],
                [2, "Table 2", {}],
            ],
            columns=["id", "name", "data"],
        )
        expected_tables["run__id"] = run.id
        expected_tables["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_tables["created_by"] = "@unknown"

        tables = service.tabulate()
        pdt.assert_frame_equal(tables, expected_tables, check_like=True)
