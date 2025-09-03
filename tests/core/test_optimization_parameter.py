import warnings
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pytest

import ixmp4
from ixmp4.core import IndexSet, Parameter
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
    RunLockRequired,
)

from .. import utils

if TYPE_CHECKING:
    from ixmp4.data.backend import SqlAlchemyBackend


def df_from_list(parameters: list[Parameter]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                parameter.data,
                parameter.name,
                parameter.run_id,
                parameter.id,
                parameter.created_at,
                parameter.created_by,
            ]
            for parameter in parameters
        ],
        columns=[
            "data",
            "name",
            "run__id",
            "id",
            "created_at",
            "created_by",
        ],
    )


class TestCoreParameter:
    def test_create_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=platform, run_id=run.id
            )
        )
        with run.transact("Test parameters.create()"):
            parameter = run.optimization.parameters.create(
                name="Parameter",
                constrained_to_indexsets=[indexset_1.name],
            )

        assert parameter.run_id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}  # JsonDict type currently requires a dict, not None
        assert parameter.column_names is None
        assert parameter.indexset_names == [indexset_1.name]
        assert parameter.values == []
        assert parameter.units == []

        # Test create without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.parameters.create(
                "Parameter 2", constrained_to_indexsets=[indexset_1.name]
            )

        with run.transact("Test parameters.create() errors and column_names"):
            # Test duplicate name raises
            with pytest.raises(Parameter.NotUnique):
                _ = run.optimization.parameters.create(
                    "Parameter", constrained_to_indexsets=[indexset_1.name]
                )

            # Test mismatch in constrained_to_indexsets and column_names raises
            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                _ = run.optimization.parameters.create(
                    "Parameter 2",
                    constrained_to_indexsets=[indexset_1.name],
                    column_names=["Dimension 1", "Dimension 2"],
                )

            # Test columns_names are used for names if given
            parameter_2 = run.optimization.parameters.create(
                "Parameter 2",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )

        assert parameter_2.column_names == ["Column 1"]

        with run.transact("Test parameters.create() multiple column_names"):
            # Test duplicate column_names raise
            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                _ = run.optimization.parameters.create(
                    name="Parameter 3",
                    constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                    column_names=["Column 1", "Column 1"],
                )

            # Test using different column names for same indexset
            parameter_3 = run.optimization.parameters.create(
                name="Parameter 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 2"],
            )

        assert parameter_3.column_names == ["Column 1", "Column 2"]
        assert parameter_3.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset_1,) = utils.create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test parameters.delete()"):
            parameter = run.optimization.parameters.create(
                name="Parameter", constrained_to_indexsets=[indexset_1.name]
            )

            # TODO How to check that DeletionPrevented is raised? No other object uses
            # Parameter.id, so nothing could prevent the deletion.

            # Test unknown name raises
            with pytest.raises(Parameter.NotFound):
                run.optimization.parameters.delete(item="does not exist")

            # Test normal deletion
            run.optimization.parameters.delete(item=parameter.name)

        assert run.optimization.parameters.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not run.optimization.indexsets.tabulate().empty

        # Test that association table rows are deleted
        # If they haven't, this would raise DeletionPrevented
        with run.transact("Test parameters.delete() indexset linkage"):
            run.optimization.indexsets.delete(item=indexset_1.id)

        # Test delete without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.parameters.delete(item="Parameter 2")

    def test_get_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = utils.create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test parameters.get()"):
            _ = run.optimization.parameters.create(
                name="Parameter", constrained_to_indexsets=[indexset.name]
            )
        parameter = run.optimization.parameters.get(name="Parameter")
        assert parameter.run_id == run.id
        assert parameter.id == 1
        assert parameter.name == "Parameter"
        assert parameter.data == {}
        assert parameter.values == []
        assert parameter.units == []
        assert parameter.indexset_names == [indexset.name]

        with pytest.raises(Parameter.NotFound):
            _ = run.optimization.parameters.get("Parameter 2")

    def test_parameter_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Unit")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=platform, run_id=run.id
            )
        )
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "values": [3.14],
            "units": [unit.name],
        }

        with run.transact("Test Parameter.add()"):
            indexset.add(data=["foo", "bar", ""])
            indexset_2.add(data=[1, 2, 3])
            parameter = run.optimization.parameters.create(
                "Parameter",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            parameter.add(data=test_data_1)

        assert parameter.data == test_data_1
        assert parameter.values == test_data_1["values"]
        assert parameter.units == test_data_1["units"]

        test_data_2 = {
            indexset.name: ["", "", "foo", "foo", "bar", "bar"],
            indexset_2.name: [3, 1, 2, 1, 2, 3],
            "values": [6, 5, 4, 3, 2, 1],
            "units": [unit.name] * 6,
        }

        # Test add without run lock raises
        with pytest.raises(RunLockRequired):
            parameter.add(data=test_data_2)

        with run.transact("Test Parameter.add() errors and order"):
            parameter_2 = run.optimization.parameters.create(
                name="Parameter 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

            with pytest.raises(
                OptimizationItemUsageError,
                match=r"must include the column\(s\): values!",
            ):
                parameter_2.add(
                    pd.DataFrame(
                        {
                            indexset.name: [None],
                            indexset_2.name: [2],
                            "units": [unit.name],
                        }
                    ),
                )

            with pytest.raises(
                OptimizationItemUsageError,
                match=r"must include the column\(s\): units!",
            ):
                parameter_2.add(
                    data=pd.DataFrame(
                        {
                            indexset.name: [None],
                            indexset_2.name: [2],
                            "values": [""],
                        }
                    ),
                )

            # By converting data to pd.DataFrame, we automatically enforce equal length
            # of new columns, raises All arrays must be of the same length otherwise:
            with pytest.raises(
                OptimizationDataValidationError,
                match="All arrays must be of the same length",
            ):
                parameter_2.add(
                    data={
                        indexset.name: ["foo", "foo"],
                        indexset_2.name: [2, 2],
                        "values": [1, 2],
                        "units": [unit.name],
                    },
                )

            with pytest.raises(
                OptimizationDataValidationError, match="contains duplicate rows"
            ):
                parameter_2.add(
                    data={
                        indexset.name: ["foo", "foo"],
                        indexset_2.name: [2, 2],
                        "values": [1, 2],
                        "units": [unit.name, unit.name],
                    },
                )

            # Test that order is conserved
            parameter_2.add(test_data_2)

        assert parameter_2.data == test_data_2
        assert parameter_2.values == test_data_2["values"]
        assert parameter_2.units == test_data_2["units"]

        unit_2 = platform.units.create("Unit 2")

        # Test updating of existing keys
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "values": [1, "2", 2.3, "4"],
            "units": [unit.name] * 4,
        }
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "values": [1, 2.3, 3, 4, "5"],
            "units": [unit.name] * 2 + [unit_2.name] * 3,
        }

        with run.transact("Test Parameter.add() update"):
            parameter_4 = run.optimization.parameters.create(
                name="Parameter 4",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            parameter_4.add(data=test_data_6)
            parameter_4.add(data=test_data_7)

        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset.name, indexset_2.name])
            )
            .reset_index()
        )
        utils.assert_unordered_equality(expected, pd.DataFrame(parameter_4.data))

        # Test adding with column_names
        test_data_8 = {
            "Column 1": ["", "", "foo", "foo", "bar", "bar"],
            "Column 2": [3, 1, 2, 1, 2, 3],
            "values": [6, 5, 4, 3, 2, 1],
            "units": [unit.name] * 6,
        }

        with run.transact("Test Parameter.add() column_names"):
            parameter_5 = run.optimization.parameters.create(
                name="Parameter 5",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
                column_names=["Column 1", "Column 2"],
            )
            parameter_5.add(data=test_data_8)

        assert parameter_5.data == test_data_8

        # Test adding empty data works
        with run.transact("Test Parameter.add() empty"):
            parameter_5.add(pd.DataFrame())

        assert parameter_5.data == test_data_8

    def test_parameter_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Unit")
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=platform, run_id=run.id
            )
        )
        initial_data: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo", "foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 1, 2, 3],
            "values": [1, 2, 3, 4, 5, 6],
            "units": [unit.name] * 6,
        }

        with run.transact("Test Parameter.remove() -- preparation"):
            indexset_1.add(data=["foo", "bar", ""])
            indexset_2.add(data=[1, 2, 3])
            parameter = run.optimization.parameters.create(
                name="Parameter",
                constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            )
            parameter.add(data=initial_data)

        # Test removing empty data removes nothing
        with run.transact("Test Parameter.remove() empty"):
            parameter.remove(data={})

        assert parameter.data == initial_data

        # Test remove without run lock raises
        with pytest.raises(RunLockRequired):
            parameter.remove(data={})

        remove_data_1: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo"],
            indexset_2.name: [1],
        }

        with run.transact("Test Parameter.remove() errors and single"):
            # Test incomplete index raises
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                parameter.remove(data={indexset_1.name: ["foo"]})

            # Test unknown keys without indexed columns raises...
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                parameter.remove(data={"foo": ["bar"]})

            # ...even when removing a column that's known in principle
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                parameter.remove(data={"units": [unit.name]})

            # Test removing one row
            parameter.remove(data=remove_data_1)

        # Prepare the expectation from the original test data
        # You can confirm manually that only the correct types are removed
        for key in remove_data_1.keys():
            initial_data[key].remove(remove_data_1[key][0])  # type: ignore[arg-type]
        initial_data["values"].remove(1)  # type: ignore[arg-type]
        initial_data["units"].remove(unit.name)  # type: ignore[arg-type]

        assert parameter.data == initial_data

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data_1["values"] = [1]
        with run.transact("Test Parameter.remove() non-existing"):
            parameter.remove(data=remove_data_1)

        assert parameter.data == initial_data

        # Test removing multiple rows
        remove_data_2 = pd.DataFrame(
            {indexset_1.name: ["foo", "bar", "bar"], indexset_2.name: [3, 1, 3]}
        )
        with run.transact("Test Parameter.remove() multiple"):
            parameter.remove(data=remove_data_2)

        # Prepare the expectation
        expected = {
            indexset_1.name: ["foo", "bar"],
            indexset_2.name: [2, 2],
            "values": [2, 5],
            "units": [unit.name] * 2,
        }

        assert parameter.data == expected

        # Test removing all remaining data
        remove_data_3 = {indexset_1.name: ["foo", "bar"], indexset_2.name: [2, 2]}
        with run.transact("Test Parameter.remove() all data"):
            parameter.remove(data=remove_data_3)

        assert parameter.data == {}

    def test_list_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        utils.create_indexsets_for_run(platform=platform, run_id=run.id)
        with run.transact("Test parameters.list()"):
            parameter = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=["Indexset 1"]
            )
            parameter_2 = run.optimization.parameters.create(
                "Parameter 2", constrained_to_indexsets=["Indexset 2"]
            )

        # Create new run to test listing parameters for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset,) = utils.create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        with run_2.transact("Test parameters.list() for specific run"):
            run_2.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset.name]
            )
        expected_ids = [parameter.id, parameter_2.id]
        list_ids = [parameter.id for parameter in run.optimization.parameters.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [parameter.id]
        list_id = [
            parameter.id
            for parameter in run.optimization.parameters.list(name="Parameter")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=platform, run_id=run.id
            )
        )
        with run.transact("Test parameters.tabulate()"):
            parameter = run.optimization.parameters.create(
                name="Parameter",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            parameter_2 = run.optimization.parameters.create(
                name="Parameter 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
        # Create new run to test listing parameters for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset_3,) = utils.create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        with run_2.transact("Test parameters.tabulate() for specific run"):
            run_2.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset_3.name]
            )
        pd.testing.assert_frame_equal(
            df_from_list([parameter_2]),
            run.optimization.parameters.tabulate(name="Parameter 2"),
        )

        unit = platform.units.create("Unit")
        unit_2 = platform.units.create("Unit 2")
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "values": ["value"],
            "units": [unit.name],
        }
        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "values": [1, "value"],
            "units": [unit.name, unit_2.name],
        }

        with run.transact("Test parameters.tabulate() with data"):
            indexset.add(data=["foo", "bar"])
            indexset_2.add(data=[1, 2, 3])
            parameter.add(data=test_data_1)
            parameter_2.add(data=test_data_2)

        pd.testing.assert_frame_equal(
            df_from_list([parameter, parameter_2]),
            run.optimization.parameters.tabulate(),
        )

    def test_parameter_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = utils.create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test Parameter.docs"):
            parameter_1 = run.optimization.parameters.create(
                "Parameter 1", constrained_to_indexsets=[indexset.name]
            )
        docs = "Documentation of Parameter 1"
        parameter_1.docs = docs
        assert parameter_1.docs == docs

        parameter_1.docs = None
        assert parameter_1.docs is None

    def test_parameter_rollback_sqlite(self, sqlite_platform: ixmp4.Platform) -> None:
        run = sqlite_platform.runs.create("Model", "Scenario")
        (indexset,) = tuple(
            IndexSet(_backend=sqlite_platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=sqlite_platform, run_id=run.id, amount=1
            )
        )
        test_data = {indexset.name: ["foo"]}

        with run.transact("Test Parameter versioning"):
            parameter = run.optimization.parameters.create(
                "Parameter 1", constrained_to_indexsets=[indexset.name]
            )
            indexset.add(["foo"])

        with warnings.catch_warnings(record=True) as w:
            try:
                with (
                    run.transact("Test Parameter versioning update on sqlite"),
                ):
                    parameter.add(test_data)
                    raise utils.CustomException("Whoops!!!")
            except utils.CustomException:
                pass

        parameter = run.optimization.parameters.get(parameter.name)

        assert parameter.data == test_data
        assert (
            "An exception occurred but the `Run` was not reverted because "
            "versioning is not supported by this platform" in str(w[0].message)
        )

    def test_versioning_parameter(self, pg_platform: ixmp4.Platform) -> None:
        run = pg_platform.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=pg_platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=pg_platform, run_id=run.id
            )
        )
        unit = pg_platform.units.create("Unit")
        unit_2 = pg_platform.units.create("Unit 2")

        with run.transact("Test Parameter versioning"):
            indexset_1.add(data=[1, 2, 3])
            indexset_2.add(data=["foo", "bar"])
            parameter = run.optimization.parameters.create(
                "Parameter 1",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )
            parameter.docs = "Docs of Parameter 1"
            parameter.add(
                data={
                    "Column 1": [1, 2],
                    "values": [1, 2],
                    "units": [unit.name, unit.name],
                }
            )
            parameter.add(data={"Column 1": [3], "values": [3], "units": [unit.name]})
            parameter_2 = run.optimization.parameters.create(
                name="Parameter 2", constrained_to_indexsets=[indexset_2.name]
            )
            parameter_2.add(
                data={
                    indexset_2.name: ["foo", "bar"],
                    "values": [4, 5],
                    "units": [unit_2.name, unit_2.name],
                }
            )
            parameter_2.remove(data={indexset_2.name: ["foo"]})
            run.optimization.parameters.delete(parameter_2.id)

            @utils.versioning_test(pg_platform.backend)
            def assert_versions(backend: "SqlAlchemyBackend") -> None:
                # Test Parameter versions
                vdf = backend.optimization.parameters.versions.tabulate()

                data = vdf["data"].to_list()

                # TODO assert_unordered_equality can't handle dict for .data
                # property/column. Should we switch it to nullable? How do we test here?
                expected = (
                    pd.read_csv(
                        "./tests/core/expected_versions/test_parameter_versioning.csv"
                    )
                    .replace({np.nan: None})
                    .assign(
                        created_at=pd.Series(
                            [
                                parameter.created_at,
                                parameter.created_at,
                                parameter.created_at,
                                parameter_2.created_at,
                                parameter_2.created_at,
                                parameter_2.created_at,
                                parameter_2.created_at,
                            ]
                        )
                    )
                )

                # NOTE Don't know how to store/read in these dicts with csv
                expected_data = [
                    {},
                    {
                        "Column 1": [1, 2],
                        "values": [1, 2],
                        "units": [unit.name, unit.name],
                    },
                    {
                        "Column 1": [1, 2, 3],
                        "values": [1, 2, 3],
                        "units": [unit.name, unit.name, unit.name],
                    },
                    {},
                    {
                        "Indexset 2": ["foo", "bar"],
                        "values": [4, 5],
                        "units": [unit_2.name, unit_2.name],
                    },
                    {"Indexset 2": ["bar"], "values": [5], "units": [unit_2.name]},
                    {"Indexset 2": ["bar"], "values": [5], "units": [unit_2.name]},
                ]

                utils.assert_unordered_equality(expected, vdf.drop(columns="data"))
                assert data == expected_data

                # Test ParameterIndexSetAssociation versions
                # NOTE The last entry here comes implicitly from deleting Parameter 2
                vdf = backend.optimization.parameters._associations.versions.tabulate()

                expected = pd.read_csv(
                    "./tests/core/expected_versions/test_parameterindexsetassociations_versioning.csv"
                ).replace({np.nan: None})

                utils.assert_unordered_equality(expected, vdf)

    def test_parameter_rollback(self, pg_platform: ixmp4.Platform) -> None:
        run = pg_platform.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=pg_platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=pg_platform, run_id=run.id
            )
        )
        unit = pg_platform.units.create("Unit")

        # Test rollback of Parameter creation
        try:
            with run.transact("Test Parameter rollback on creation"):
                _ = run.optimization.parameters.create(
                    "Parameter", constrained_to_indexsets=[indexset_1.name]
                )
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        assert run.optimization.parameters.tabulate().empty

        # Test rollback of Parameter creation when linked in Docs table
        try:
            with run.transact("Test Parameter rollback after setting docs"):
                parameter = run.optimization.parameters.create(
                    "Parameter", constrained_to_indexsets=[indexset_1.name]
                )
                parameter.docs = "Test Parameter"
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        assert pg_platform.backend.optimization.parameters.docs.tabulate().empty

        with run.transact("Test Parameter rollback setup"):
            parameter = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset_1.name]
            )
            indexset_1.add(data=[1, 2, 3])
            parameter_2 = run.optimization.parameters.create(
                "Parameter 2",
                constrained_to_indexsets=[indexset_2.name],
                column_names=["Column 2"],
            )
            indexset_2.add(data=["foo", "bar"])

        test_data = {
            indexset_1.name: [1, 3],
            "values": [1.0, 3.0],
            "units": [unit.name, unit.name],
        }

        # Test rollback of Parameter data addition
        try:
            with run.transact("Test Parameter rollback on data addition"):
                parameter.add(data=test_data)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter = run.optimization.parameters.get("Parameter")
        assert parameter.data == {}

        # Test rollback of Parameter data removal
        with run.transact("Test Parameter rollback on data removal -- setup"):
            parameter.add(data=test_data)

        try:
            with run.transact("Test Parameter rollback on data removal"):
                parameter.remove(data={indexset_1.name: [1]})
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter = run.optimization.parameters.get("Parameter")
        assert parameter.data == test_data

        # Test rollback of Parameter deletion
        try:
            with run.transact("Test Parameter rollback on deletion"):
                run.optimization.parameters.delete("Parameter")
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter = run.optimization.parameters.get("Parameter")
        assert parameter.indexset_names == [indexset_1.name]
        assert parameter.data == test_data

        # Test rollback of Parameter deletion with column_names
        try:
            with run.transact("Test Parameter rollback on deletion with column_names"):
                run.optimization.parameters.delete("Parameter 2")
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter_2 = run.optimization.parameters.get("Parameter 2")
        assert parameter_2.indexset_names == [indexset_2.name]
        assert parameter_2.column_names == ["Column 2"]

        # Test rollback of Parameter deletion with IndexSet deletion
        try:
            with run.transact(
                "Test Parameter rollback on deletion w/ IndexSet deletion"
            ):
                run.optimization.parameters.delete("Parameter")
                run.optimization.indexsets.delete(indexset_1.name)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter = run.optimization.parameters.get("Parameter")
        assert parameter.indexset_names == [indexset_1.name]
        assert parameter.data == test_data

        # Test rollback of Parameter deletion with Unit deletion
        try:
            with run.transact(
                "Test Parameter rollback on deletion w/ Unit deletion",
                revert_platform_on_error=True,
            ):
                run.optimization.parameters.delete(parameter.id)
                pg_platform.units.delete(unit)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter = run.optimization.parameters.get("Parameter")
        assert parameter.units[0] == unit.name

        # NOTE Without revert_platform_on_error above, this would not work
        unit = pg_platform.units.get(unit.name)
        assert unit.id is not None

    def test_parameter_rollback_to_checkpoint(
        self, pg_platform: ixmp4.Platform
    ) -> None:
        run = pg_platform.runs.create("Model", "Scenario")
        (indexset,) = tuple(
            IndexSet(_backend=pg_platform.backend, _model=model, _run=run)
            for model in utils.create_indexsets_for_run(
                platform=pg_platform, run_id=run.id, amount=1
            )
        )
        unit = pg_platform.units.create("Unit")
        test_data = {
            indexset.name: [1, 3],
            "values": [1.0, 3.0],
            "units": [unit.name, unit.name],
        }

        try:
            with run.transact("Test Parameter rollback to checkpoint"):
                parameter = run.optimization.parameters.create(
                    "Parameter", constrained_to_indexsets=[indexset.name]
                )
                indexset.add(data=[1, 2, 3])
                parameter.add(data=test_data)
                run.checkpoints.create("Test Parameter rollback to checkpoint")
                parameter.remove(data={indexset.name: [1]})
                run.optimization.parameters.delete(item=parameter.id)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        parameter = run.optimization.parameters.get("Parameter")
        assert parameter.data == test_data
