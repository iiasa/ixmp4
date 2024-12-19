import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.core import IndexSet
from ixmp4.core.exceptions import OptimizationDataValidationError

from ..utils import create_indexsets_for_run


def df_from_list(indexsets: list[IndexSet], include_data: bool = False) -> pd.DataFrame:
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
    if include_data:
        result.insert(
            loc=0, column="data", value=[indexset.data for indexset in indexsets]
        )
    else:
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
        indexset_1 = run.optimization.indexsets.create("Indexset 1")
        assert indexset_1.id == 1
        assert indexset_1.name == "Indexset 1"

        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        assert indexset_1.id != indexset_2.id

        with pytest.raises(IndexSet.NotUnique):
            _ = run.optimization.indexsets.create("Indexset 1")

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
        indexset_1 = run.optimization.indexsets.create("Indexset 1")
        indexset_1.add(test_data)
        run.optimization.indexsets.create("Indexset 2").add(test_data)
        indexset_2 = run.optimization.indexsets.get("Indexset 2")

        assert indexset_1.data == indexset_2.data

        with pytest.raises(OptimizationDataValidationError):
            indexset_1.add(["baz", "foo"])

        with pytest.raises(OptimizationDataValidationError):
            indexset_2.add(["baz", "baz"])

        # Test data types are conserved
        indexset_3 = run.optimization.indexsets.create("Indexset 3")
        test_data_2 = [1.2, 3.4, 5.6]
        indexset_3.add(data=test_data_2)

        assert indexset_3.data == test_data_2
        assert type(indexset_3.data[0]).__name__ == "float"

        indexset_4 = run.optimization.indexsets.create("Indexset 4")
        test_data_3 = [0, 1, 2]
        indexset_4.add(data=test_data_3)

        assert indexset_4.data == test_data_3
        assert type(indexset_4.data[0]).__name__ == "int"

    def test_list_indexsets(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        # Create indexset in another run to test listing indexsets for specific run
        platform.runs.create("Model", "Scenario").optimization.indexsets.create(
            "Indexset 1"
        )
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
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        # Create indexset in another run to test tabulating indexsets for specific run
        platform.runs.create("Model", "Scenario").optimization.indexsets.create(
            "Indexset 1"
        )

        expected = df_from_list(indexsets=[indexset_1, indexset_2])
        result = run.optimization.indexsets.tabulate()
        # utils.assert_unordered_equality doesn't like lists, so make sure the order in
        # df_from_list() is correct!
        pdt.assert_frame_equal(expected, result)

        expected = df_from_list(indexsets=[indexset_2])
        result = run.optimization.indexsets.tabulate(name="Indexset 2")
        pdt.assert_frame_equal(expected, result)

        # Test tabulating including the data
        expected = df_from_list(indexsets=[indexset_2], include_data=True)
        pdt.assert_frame_equal(
            expected,
            run.optimization.indexsets.tabulate(
                name=indexset_2.name, include_data=True
            ),
        )

    def test_indexset_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset_1,) = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(
                platform=platform, run_id=run.id, amount=1
            )
        )
        docs = "Documentation of Indexset 1"
        indexset_1.docs = docs
        assert indexset_1.docs == docs

        indexset_1.docs = None
        assert indexset_1.docs is None
