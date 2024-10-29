import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data.abstract import IndexSet

from ..utils import create_indexsets_for_run


def df_from_list(indexsets: list[IndexSet], include_data: bool = False) -> pd.DataFrame:
    result = pd.DataFrame(
        # Order is important here to avoid utils.assert_unordered_equality,
        # which doesn't like lists
        [
            [
                indexset.run__id,
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


class TestDataOptimizationIndexSet:
    def test_create_indexset(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1 = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        assert indexset_1.id == 1
        assert indexset_1.run__id == 1
        assert indexset_1.name == "Indexset"

        with pytest.raises(IndexSet.NotUnique):
            _ = platform.backend.optimization.indexsets.create(
                run_id=run.id, name="Indexset"
            )

    def test_get_indexset(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=platform, run_id=run.id, amount=1)
        indexset = platform.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset 1"
        )
        assert indexset.id == 1
        assert indexset.run__id == 1
        assert indexset.name == "Indexset 1"

        with pytest.raises(IndexSet.NotFound):
            _ = platform.backend.optimization.indexsets.get(
                run_id=run.id, name="Indexset 2"
            )

    def test_list_indexsets(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        assert [indexset_1] == platform.backend.optimization.indexsets.list(
            name=indexset_1.name
        )
        assert [
            indexset_1,
            indexset_2,
        ] == platform.backend.optimization.indexsets.list()

        # Test only indexsets belonging to this Run are listed when run_id is provided
        run_2 = platform.backend.runs.create("Model", "Scenario")
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, offset=2
        )
        assert [indexset_3, indexset_4] == platform.backend.optimization.indexsets.list(
            run_id=run_2.id
        )

    def test_tabulate_indexsets(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_1.id, data="foo"
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_2.id, data=[1, 2]
        )

        indexset_1 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_1.name
        )
        indexset_2 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_2.name
        )
        expected = df_from_list(indexsets=[indexset_1, indexset_2])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.indexsets.tabulate()
        )

        expected = df_from_list(indexsets=[indexset_1])
        pdt.assert_frame_equal(
            expected,
            platform.backend.optimization.indexsets.tabulate(name=indexset_1.name),
        )

        # Test only indexsets belonging to this Run are tabulated if run_id is provided
        run_2 = platform.backend.runs.create("Model", "Scenario")
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, offset=3
        )
        expected = df_from_list(indexsets=[indexset_3, indexset_4])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.indexsets.tabulate(run_id=run_2.id)
        )

        # Test tabulating including the data
        expected = df_from_list(indexsets=[indexset_2], include_data=True)
        pdt.assert_frame_equal(
            expected,
            platform.backend.optimization.indexsets.tabulate(
                name=indexset_2.name, include_data=True
            ),
        )

    def test_add_data(self, platform: ixmp4.Platform):
        test_data = ["foo", "bar"]
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_1.id,
            data=test_data,  # type: ignore
        )
        indexset_1 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_1.name
        )

        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_2.id,
            data=test_data,  # type: ignore
        )

        assert (
            indexset_1.data
            == platform.backend.optimization.indexsets.get(
                run_id=run.id, name=indexset_2.name
            ).data
        )

        with pytest.raises(OptimizationDataValidationError):
            platform.backend.optimization.indexsets.add_data(
                indexset_id=indexset_1.id, data=["baz", "foo"]
            )

        with pytest.raises(OptimizationDataValidationError):
            platform.backend.optimization.indexsets.add_data(
                indexset_id=indexset_2.id, data=["baz", "baz"]
            )

        # Test data types are conserved
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run.id, offset=3
        )

        test_data_2: list[float | int | str] = [1.2, 3.4, 5.6]
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_3.id, data=test_data_2
        )
        indexset_3 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_3.name
        )

        assert indexset_3.data == test_data_2
        assert type(indexset_3.data[0]).__name__ == "float"

        test_data_3: list[float | int | str] = [0, 1, 2]
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_4.id, data=test_data_3
        )
        indexset_4 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_4.name
        )

        assert indexset_4.data == test_data_3
        assert type(indexset_4.data[0]).__name__ == "int"
