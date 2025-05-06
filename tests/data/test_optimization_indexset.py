import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data.abstract import IndexSet

from ..utils import assert_logs, create_indexsets_for_run


def df_from_list(indexsets: list[IndexSet]) -> pd.DataFrame:
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
    def test_create_indexset(self, platform: ixmp4.Platform) -> None:
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

    def test_get_indexset(self, platform: ixmp4.Platform) -> None:
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

    def test_add_data(self, platform: ixmp4.Platform) -> None:
        test_data = ["foo", "bar"]
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_1.id, data=test_data
        )
        indexset_1 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_1.name
        )

        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=test_data
        )

        assert (
            indexset_1.data
            == platform.backend.optimization.indexsets.get(
                run_id=run.id, name=indexset_2.name
            ).data
        )

        with pytest.raises(OptimizationDataValidationError):
            platform.backend.optimization.indexsets.add_data(
                id=indexset_1.id, data=["baz", "foo"]
            )

        with pytest.raises(OptimizationDataValidationError):
            platform.backend.optimization.indexsets.add_data(
                id=indexset_2.id, data=["baz", "baz"]
            )

        # Test data types are conserved
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run.id, offset=3
        )

        test_data_2 = [1.2, 3.4, 5.6]
        platform.backend.optimization.indexsets.add_data(
            id=indexset_3.id, data=test_data_2
        )
        indexset_3 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_3.name
        )

        assert indexset_3.data == test_data_2
        assert type(indexset_3.data[0]).__name__ == "float"

        test_data_3 = [0, 1, 2]
        platform.backend.optimization.indexsets.add_data(
            id=indexset_4.id, data=test_data_3
        )
        indexset_4 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_4.name
        )

        assert indexset_4.data == test_data_3
        assert type(indexset_4.data[0]).__name__ == "int"

    def test_remove_data(
        self, platform: ixmp4.Platform, caplog: pytest.LogCaptureFixture
    ) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        test_data = ["do", "re", "mi", "fa", "so", "la", "ti"]
        platform.backend.optimization.indexsets.add_data(id=indexset.id, data=test_data)

        # Test removing multiple arbitrary known data
        remove_data = ["do", "mi", "la", "ti"]
        expected = [data for data in test_data if data not in remove_data]
        platform.backend.optimization.indexsets.remove_data(
            id=indexset.id, data=remove_data
        )
        indexset = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset.name
        )

        assert indexset.data == expected

        # Test removing single item
        expected.remove("fa")
        platform.backend.optimization.indexsets.remove_data(id=indexset.id, data="fa")
        indexset = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset.name
        )

        assert indexset.data == expected

        # Test removing unknown data logs messages
        with assert_logs(
            caplog=caplog,
            message_or_messages=[
                "No data were removed!",
                "Not all items in `data` were registered",
            ],
            at_level="INFO",
        ):
            # Test completely unknown data
            platform.backend.optimization.indexsets.remove_data(
                id=indexset.id, data=["foo"]
            )
            # Test partly unknown data
            platform.backend.optimization.indexsets.remove_data(
                id=indexset.id, data=["foo", "so"]
            )

        # Test removing all remaining data
        platform.backend.optimization.indexsets.remove_data(
            id=indexset.id, data=["so", "re"]
        )
        indexset = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset.name
        )

        assert indexset.data == []

    def test_list_indexsets(self, platform: ixmp4.Platform) -> None:
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

    def test_tabulate_indexsets(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(id=indexset_1.id, data="foo")
        platform.backend.optimization.indexsets.add_data(id=indexset_2.id, data=[1, 2])

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
