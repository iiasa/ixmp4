import logging

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.core import IndexSet
from ixmp4.core.exceptions import (
    DeletionPrevented,
    OptimizationDataValidationError,
    RunLockRequired,
)

from ..utils import CustomException, assert_unordered_equality, create_indexsets_for_run


def df_from_list(indexsets: list[IndexSet]) -> pd.DataFrame:
    result = pd.DataFrame(
        # Order is important here to avoid utils.assert_unordered_equality,
        # which doesn't like lists
        [
            [
                indexset.run_id,
                indexset.name,
                indexset.id,
                indexset.created_at,
                indexset.created_by,
            ]
            for indexset in indexsets
        ],
        columns=[
            "run__id",
            "name",
            "id",
            "created_at",
            "created_by",
        ],
    )
    result.insert(
        loc=0,
        column="data_type",
        value=[
            type(indexset.data[0]).__name__ if indexset.data != [] else None
            for indexset in indexsets
        ],
    )
    return result


class TestCoreIndexset:
    def test_create_indexset(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        with run.transact("Test indexsets.create()"):
            indexset_1 = run.optimization.indexsets.create("Indexset 1")
        assert indexset_1.id == 1
        assert indexset_1.name == "Indexset 1"

        # Test create without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.indexsets.create("Indexset 2")

        with run.transact("Test indexsets.create() 2"):
            indexset_2 = run.optimization.indexsets.create("Indexset 2")
        assert indexset_1.id != indexset_2.id

        with run.transact("Test indexsets.create() error"):
            with pytest.raises(IndexSet.NotUnique):
                _ = run.optimization.indexsets.create("Indexset 1")

    def test_delete_indexset(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        with run.transact("Test indexsets.delete()"):
            indexset_1 = run.optimization.indexsets.create(name="Indexset")
            run.optimization.indexsets.delete(item=indexset_1.name)

        # Test normal deletion
        assert run.optimization.indexsets.tabulate().empty

        with run.transact("Test indexsets.delete() NotFound"):
            # Test unknown id raises
            with pytest.raises(IndexSet.NotFound):
                run.optimization.indexsets.delete(item="does not exist")

        # Test DeletionPrevented is raised when IndexSet is used somewhere
        (indexset_2,) = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(
                platform=platform, run_id=run.id, amount=1
            )
        )
        with run.transact("Test indexsets.create() DeletionPrevented"):
            with pytest.raises(DeletionPrevented):
                _ = run.optimization.tables.create(
                    name="Table 1", constrained_to_indexsets=[indexset_2.name]
                )
                run.optimization.indexsets.delete(item=indexset_2.id)

        # Test delete without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.indexsets.delete(item="Indexset 2")

    def test_get_indexset(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=platform, run_id=run.id, amount=1)
        indexset = run.optimization.indexsets.get("Indexset 1")
        assert indexset.id == 1
        assert indexset.name == "Indexset 1"

        with pytest.raises(IndexSet.NotFound):
            _ = run.optimization.indexsets.get("Foo")

    def test_add_elements(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        test_data = ["foo", "bar"]
        with run.transact("Test Indexset.add()"):
            indexset_1 = run.optimization.indexsets.create("Indexset 1")
            indexset_1.add(test_data)
            run.optimization.indexsets.create("Indexset 2").add(test_data)
            indexset_2 = run.optimization.indexsets.get("Indexset 2")

        assert indexset_1.data == indexset_2.data

        with run.transact("Test Indexset.add() errors"):
            with pytest.raises(OptimizationDataValidationError):
                indexset_1.add(["baz", "foo"])

            with pytest.raises(OptimizationDataValidationError):
                indexset_2.add(["baz", "baz"])

        # Test add without run lock raises
        with pytest.raises(RunLockRequired):
            indexset_1.add(["baz", "foo"])

        # Test data types are conserved
        test_data_2 = [1.2, 3.4, 5.6]
        with run.transact("Test Indexset.add() data types"):
            indexset_3 = run.optimization.indexsets.create("Indexset 3")
            indexset_3.add(data=test_data_2)

        assert indexset_3.data == test_data_2
        assert type(indexset_3.data[0]).__name__ == "float"

        test_data_3 = [0, 1, 2]
        with run.transact("Test Indexset.add() data types 2"):
            indexset_4 = run.optimization.indexsets.create("Indexset 4")
            indexset_4.add(data=test_data_3)

        assert indexset_4.data == test_data_3
        assert type(indexset_4.data[0]).__name__ == "int"

        with run.transact("Test Indexset.add() empty data"):
            # Test adding empty data works
            indexset_4.add(data=[])

        assert indexset_4.data == test_data_3

    def test_remove_elements(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        test_data = ["do", "re", "mi", "fa", "so", "la", "ti"]
        with run.transact("Test Indexset.remove()"):
            indexset_1 = run.optimization.indexsets.create("Indexset 1")
            indexset_1.add(test_data)

            # Test removing an empty list removes nothing
            indexset_1.remove(data=[])

        assert indexset_1.data == test_data

        # Test remove without run lock raises
        with pytest.raises(RunLockRequired):
            indexset_1.remove(data=[])

        # Define additional items affected by `remove_data`
        unit = platform.units.create("Unit")
        # Test removing multiple arbitrary known data
        remove_data = ["fa", "mi", "la", "ti"]
        expected = ["do", "re", "so"]
        expected_table = ["do", "re"]
        expected_parameter = {
            indexset_1.name: ["so"],
            "values": [3],
            "units": [unit.name],
        }
        with run.transact("Test Indexset.remove() linked items"):
            # Define a basic affected Table
            table = run.optimization.tables.create(
                "Table", constrained_to_indexsets=[indexset_1.name]
            )
            table.add({indexset_1.name: ["do", "re", "mi"]})

            # Define an affected Table without data
            table_2 = run.optimization.tables.create(
                "Table 2", constrained_to_indexsets=[indexset_1.name]
            )

            # Define a basic affected Parameter
            parameter = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset_1.name]
            )
            parameter.add(
                {
                    indexset_1.name: ["mi", "fa", "so"],
                    "values": [1, 2, 3],
                    "units": [unit.name] * 3,
                }
            )

            # Define a Parameter where only 1 dimension is affected
            indexset_2 = run.optimization.indexsets.create("Indexset 2")
            indexset_2.add(["foo", "bar", "baz"])
            parameter_2 = run.optimization.parameters.create(
                "Parameter 2",
                constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            )
            parameter_2.add(
                {
                    indexset_1.name: ["do", "do", "la", "ti"],
                    indexset_2.name: ["foo", "bar", "baz", "foo"],
                    "values": [1, 2, 3, 4],
                    "units": [unit.name] * 4,
                }
            )

            # Define a Parameter with 2 affected dimensions
            parameter_3 = run.optimization.parameters.create(
                "Parameter 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 2"],
            )
            parameter_3.add(
                {
                    "Column 1": ["la", "la", "do", "ti"],
                    "Column 2": ["re", "fa", "mi", "do"],
                    "values": [1, 2, 3, 4],
                    "units": [unit.name, unit.name, unit.name, unit.name],
                }
            )

            indexset_1.remove(data=remove_data)

        assert indexset_1.data == expected

        # NOTE Manual reloading is not actually necessary when using the DB layer
        # directly, but we should document this as necessary because we would have to
        # build something close to an sqla-like object tracking system for the API layer
        # otherwise
        table = run.optimization.tables.get(table.name)
        parameter = run.optimization.parameters.get(parameter.name)
        parameter_2 = run.optimization.parameters.get(parameter_2.name)
        parameter_3 = run.optimization.parameters.get(parameter_3.name)

        # Test effect on linked items
        expected_parameter_2 = {
            indexset_1.name: ["do", "do"],
            indexset_2.name: ["foo", "bar"],
            "values": [1, 2],
            "units": [unit.name] * 2,
        }
        assert table.data[indexset_1.name] == expected_table
        assert parameter.data == expected_parameter
        assert parameter_2.data == expected_parameter_2
        assert parameter_3.data == {}

        # Test removing a single item
        expected.remove("do")
        expected_table.remove("do")
        with run.transact("Test Indexset.remove() linked items single"):
            indexset_1.remove(data="do")

        assert indexset_1.data == expected

        with run.transact("Test Indexset.remove() linked items non-existing"):
            # Test removing non-existing data removes nothing
            indexset_1.remove(data="fa")

        assert indexset_1.data == expected

        with run.transact("Test Indexset.remove() linked items wrong type"):
            # Test removing wrong type removes nothing (via conversion to unknown str)
            # NOTE Why does mypy not prevent this?
            indexset_1.remove(data=True)

        assert indexset_1.data == expected

        table = run.optimization.tables.get(table.name)
        parameter_2 = run.optimization.parameters.get(parameter_2.name)

        assert table.data[indexset_1.name] == expected_table
        assert parameter_2.data == {}

        with run.transact("Test Indexset.remove() linked items all data"):
            # Test removing all remaining data
            indexset_1.remove(data=["so", "re"], remove_dependent_data=False)

        assert indexset_1.data == []

        table = run.optimization.tables.get(table.name)
        table_2 = run.optimization.tables.get(table_2.name)
        parameter = run.optimization.parameters.get(parameter.name)

        assert table_2.data == {}

        # Test dependent items were not changed
        assert table.data[indexset_1.name] == expected_table
        assert parameter.data == expected_parameter

    def test_list_indexsets(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        # Create indexset in another run to test listing indexsets for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        with run_2.transact("Test indexsets.list()"):
            run_2.optimization.indexsets.create("Indexset 1")
        expected_ids = [indexset_1.id, indexset_2.id]
        list_ids = [indexset.id for indexset in run.optimization.indexsets.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [indexset_1.id]
        list_id = [
            indexset.id
            for indexset in run.optimization.indexsets.list(name="Indexset 1")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_indexsets(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        # Create indexset in another run to test tabulating indexsets for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        with run_2.transact("Test indexsets.tabulate()"):
            run_2.optimization.indexsets.create("Indexset 1")

        expected = df_from_list(indexsets=[indexset_1, indexset_2])
        result = run.optimization.indexsets.tabulate()
        # utils.assert_unordered_equality doesn't like lists, so make sure the order in
        # df_from_list() is correct!
        pdt.assert_frame_equal(expected, result)

        expected = df_from_list(indexsets=[indexset_2])
        result = run.optimization.indexsets.tabulate(name="Indexset 2")
        pdt.assert_frame_equal(expected, result)

    def test_indexset_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset_1,) = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(
                platform=platform, run_id=run.id, amount=1
            )
        )
        docs = "Documentation of Indexset 1"
        indexset_1.docs = docs
        assert indexset_1.docs == docs

        indexset_1.docs = None
        assert indexset_1.docs is None

    def test_versioning_indexset(self, platform: ixmp4.Platform) -> None:
        logging.basicConfig()
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

        run = platform.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )

        with run.transact("Test IndexSet versioning"):
            indexset_1.add(1)
            indexset_2.add(["foo", "bar", "baz"])

        with run.transact("Test IndexSet versioning data removal"):
            indexset_2.remove(["baz"])

        vdf = platform.backend.optimization.indexsets.tabulate_versions()

        expected = pd.DataFrame(
            [
                [
                    None,
                    run.id,
                    indexset_1.name,
                    indexset_1.id,
                    indexset_1.created_at,
                    indexset_1.created_by,
                    2,
                    5,
                    0,
                ],
                [
                    None,
                    run.id,
                    indexset_2.name,
                    indexset_2.id,
                    indexset_2.created_at,
                    indexset_2.created_by,
                    3,
                    6,
                    0,
                ],
                [
                    "int",
                    run.id,
                    indexset_1.name,
                    indexset_1.id,
                    indexset_1.created_at,
                    indexset_1.created_by,
                    5,
                    None,
                    1,
                ],
                [
                    "str",
                    run.id,
                    indexset_2.name,
                    indexset_2.id,
                    indexset_2.created_at,
                    indexset_2.created_by,
                    6,
                    10,
                    1,
                ],
                [
                    "str",
                    run.id,
                    indexset_2.name,
                    indexset_2.id,
                    indexset_2.created_at,
                    indexset_2.created_by,
                    10,
                    None,
                    1,
                ],
            ],
            columns=[
                "_data_type",
                "run__id",
                "name",
                "id",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        ).replace({np.nan: None})

        print(vdf.to_string())

        assert_unordered_equality(expected, vdf)

    def test_indexset_rollback(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )

        # Test rollback of adding data
        try:
            with run.transact("Test IndexSet rollback empty data"):
                indexset_1.add("foo")
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        indexset_1 = run.optimization.indexsets.get("Indexset 1")
        assert indexset_1.data == []

        # Test rollback of indexset creation
        try:
            with run.transact("Test IndexSet rollback on creation"):
                run.optimization.indexsets.create("Indexset 3")
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        assert [i.name for i in run.optimization.indexsets.list()] == [
            "Indexset 1",
            "Indexset 2",
        ]

        # Test rollback of indexset deletion
        try:
            with run.transact("Test IndexSet rollback on deletion"):
                run.optimization.indexsets.delete(indexset_2.id)
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        indexset_2 = run.optimization.indexsets.get("Indexset 2")
        assert indexset_2

        # Test rollback with potential id re-use
        # NOTE Skipping since we'll not use versioning on sqlite

        # Test rollback to same name
        try:
            with run.transact("Test IndexSet rollback to same name"):
                run.optimization.indexsets.delete(indexset_2.id)
                indexset_2 = run.optimization.indexsets.create("Indexset 2")
                indexset_2.add(1)
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        indexset_2 = run.optimization.indexsets.get("Indexset 2")
        assert indexset_2.data == []

        # Test resetting linked items
        # NOTE Only re-insertions of IndexSets need to be checked as they may alter the
        # IndexSet.id that is used for linking
        platform.backend.optimization.tables.create(
            run.id, "Table", constrained_to_indexsets=[indexset_1.name]
        )

        try:
            with run.transact("Test IndexSet rollback linked items"):
                run.optimization.tables.delete("Table")
                run.optimization.indexsets.delete("Indexset 1")
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        table = run.optimization.tables.get("Table")
        assert table.indexset_names == ["Indexset 1"]

    def test_indexset_rollback_to_checkpoint(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )

        # Test rollback of removing data
        try:
            with run.transact("Test IndexSet rollback all data"):
                indexset_1.add(["foo", "bar", "baz"])
                run.checkpoints.create("Test IndexSet rollback all data")
                indexset_1.remove(["foo", "bar", "baz"])
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        assert indexset_1.data == ["foo", "bar", "baz"]

        try:
            with run.transact("Test Indexset rollback partial removal"):
                indexset_1.remove("bar")
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        assert indexset_1.data == ["foo", "bar", "baz"]
