import datetime
from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import OptimizationItemUsageError
from ixmp4.data.optimization.equation.exceptions import (
    EquationDataInvalid,
    EquationNotFound,
    EquationNotUnique,
)
from ixmp4.data.optimization.equation.service import EquationService
from ixmp4.data.optimization.indexset.service import IndexSet, IndexSetService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class EquationServiceTest(ServiceTest[EquationService]):
    service_class = EquationService

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


class TestEquationCreate(EquationServiceTest):
    def test_equation_create(
        self, service: EquationService, run: Run, fake_time: datetime.datetime
    ) -> None:
        equation = service.create(run.id, "Equation")
        assert equation.run__id == run.id
        assert equation.name == "Equation"
        assert equation.data == {}
        assert equation.indexset_names is None
        assert equation.column_names is None

        assert equation.created_at == fake_time.replace(tzinfo=None)
        assert equation.created_by == "@unknown"

    def test_equation_create_versioning(
        self,
        versioning_service: EquationService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Equation",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    4,
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
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestEquationCreateInvalidArguments(EquationServiceTest):
    def test_equation_create_invalid_args(
        self,
        service: EquationService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError,
            match="Received `column_names` to name columns, but no "
            "`constrained_to_indexsets`",
        ):
            service.create(run.id, "Equation", column_names=["Column 1"])

        indexsets.create(run.id, "IndexSet")
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            service.create(
                run.id,
                "Equation",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            service.create(
                run.id,
                "Equation",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 1"],
            )


class TestEquationDeleteById(EquationServiceTest):
    def test_equation_delete_by_id(
        self,
        service: EquationService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexsets.create(run.id, "IndexSet")
        equation = service.create(
            run.id,
            "Equation",
            constrained_to_indexsets=["IndexSet"],
            column_names=["Column"],
        )
        service.delete_by_id(equation.id)
        assert service.tabulate().empty

    def test_equation_delete_by_id_versioning(
        self,
        versioning_service: EquationService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Equation",
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
                    "Equation",
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
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )
        # TODO Association Versions


class TestEquationUnique(EquationServiceTest):
    def test_equation_unique(self, service: EquationService, run: Run) -> None:
        service.create(run.id, "Equation")

        with pytest.raises(EquationNotUnique):
            service.create(run.id, "Equation")


class TestEquationGetByName(EquationServiceTest):
    def test_equation_get(self, service: EquationService, run: Run) -> None:
        equation1 = service.create(run.id, "Equation")
        equation2 = service.get(run.id, "Equation")
        assert equation1 == equation2


class TestEquationGetById(EquationServiceTest):
    def test_equation_get_by_id(self, service: EquationService, run: Run) -> None:
        equation1 = service.create(run.id, "Equation")
        equation2 = service.get_by_id(1)
        assert equation1 == equation2


class TestEquationNotFound(EquationServiceTest):
    def test_equation_not_found(self, service: EquationService, run: Run) -> None:
        with pytest.raises(EquationNotFound):
            service.get(run.id, "Equation")

        with pytest.raises(EquationNotFound):
            service.get_by_id(1)


class EquationDataTest(EquationServiceTest):
    def test_equation_add_data(
        self,
        service: EquationService,
        run: Run,
        test_data_indexsets: list[IndexSet],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        equation = service.create(
            run.id,
            "Equation",
            constrained_to_indexsets=[i.name for i in test_data_indexsets],
            column_names=column_names,
        )
        service.add_data(equation.id, test_data)
        equation = service.get_by_id(equation.id)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert equation.data == test_data

    def test_equation_remove_data_partial(
        self,
        service: EquationService,
        run: Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        equation = service.get(run.id, "Equation")
        service.remove_data(equation.id, partial_test_data)
        equation = service.get_by_id(equation.id)
        assert equation.data == remaining_test_data

    def test_equation_remove_data_all(
        self,
        service: EquationService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        equation = service.get(run.id, "Equation")
        service.remove_data(equation.id)
        equation = service.get_by_id(equation.id)
        assert equation.data == {}

    def test_equation_data_versioning(
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
        expected_versions["name"] = "Equation"
        expected_versions["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_versions["created_by"] = "@unknown"

        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestEquationData(EquationDataTest):
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
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
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
            "marginals": [-2],
            "levels": [2],
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestEquationDataWithColumnNames(EquationDataTest):
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
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
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
            "marginals": [-2],
            "levels": [2],
            "Column 1": ["do"],
            "Column 2": [3],
        }


class TestEquationDataDataFrame(EquationDataTest):
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
                [-2, 2, "do", 3],
                [1, 1, "re", 3],
                [1, 3, "mi", 1],
            ],
            columns=["marginals", "levels", "IndexSet 1", "IndexSet 2"],
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
            "marginals": [-2],
            "levels": [2],
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestEquationInvalidData(EquationServiceTest):
    def test_equations_create(
        self, service: EquationService, indexsets: IndexSetService, run: Run
    ):
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        equation = service.create(
            run.id,
            "Equation 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert equation.id == 1

        equation = service.create(
            run.id,
            "Equation 2",
        )
        assert equation.id == 2

    def test_equation_add_invalid_data(
        self,
        service: EquationService,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Equation.data must include the column\(s\): levels!",
        ):
            service.add_data(
                1,
                {
                    "marginals": [-2],
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Equation.data must include the column\(s\): marginals!",
        ):
            service.add_data(
                1,
                {
                    "levels": [2],
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Equation.data must include the column\(s\): levels, marginals!",
        ):
            service.add_data(
                1,
                {
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )

        with pytest.raises(
            EquationDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "marginals": [-2, 1, 1],
                    "levels": [2, 1, 3],
                    "IndexSet 1": ["do", "re"],  # missing "mi"
                    "IndexSet 2": [3, 3, 1],
                },
            )
        with pytest.raises(
            EquationDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "marginals": [
                        -2,
                    ],  # missing 1,1
                    "levels": [2, 1, 3],
                    "IndexSet 1": ["do", "re", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

        with pytest.raises(EquationDataInvalid, match="contains duplicate rows"):
            service.add_data(
                1,
                {
                    "marginals": [-2, 1, 1],
                    "levels": [2, 1, 3],
                    "IndexSet 1": ["do", "do", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

    def test_equation_remove_invalid_data(
        self,
        service: EquationService,
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
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            service.remove_data(
                1,
                {
                    "levels": [2],
                },
            )

        caplog.clear()
        with caplog.at_level("WARNING"):
            service.remove_data(
                2,
                {
                    "levels": [2],
                    "marginals": [-2],
                },
            )

        expected = [
            (
                "Trying to remove {'levels': [2], 'marginals': [-2]} from `Equation` "
                "'Equation 2', but that is not indexed; not removing anything!"
            )
        ]
        assert caplog.messages == expected


class TestEquationList(EquationServiceTest):
    def test_equation_list(
        self,
        service: EquationService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Equation 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(run.id, "Equation 2")

        test_data1 = {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        equations = service.list()

        assert equations[0].id == 1
        assert equations[0].run__id == run.id
        assert equations[0].name == "Equation 1"
        assert equations[0].data == test_data1
        assert equations[0].created_by == "@unknown"
        assert equations[0].created_at == fake_time.replace(tzinfo=None)

        assert equations[1].id == 2
        assert equations[1].run__id == run.id
        assert equations[1].name == "Equation 2"
        assert equations[1].data == {}
        assert equations[1].created_by == "@unknown"
        assert equations[1].created_at == fake_time.replace(tzinfo=None)


class TestEquationTabulate(EquationServiceTest):
    def test_equation_tabulate(
        self,
        service: EquationService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Equation 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(run.id, "Equation 2")

        test_data1 = {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        expected_equations = pd.DataFrame(
            [
                [1, "Equation 1", test_data1],
                [2, "Equation 2", {}],
            ],
            columns=["id", "name", "data"],
        )
        expected_equations["run__id"] = run.id
        expected_equations["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_equations["created_by"] = "@unknown"

        equations = service.tabulate()
        pdt.assert_frame_equal(equations, expected_equations, check_like=True)
