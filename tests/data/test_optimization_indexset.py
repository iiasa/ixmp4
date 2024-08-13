import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.data.abstract import IndexSet

from ..utils import create_indexsets_for_run


def df_from_list(indexsets: list):
    return pd.DataFrame(
        # Order is important here to avoid utils.assert_unordered_equality,
        # which doesn't like lists
        [
            [
                indexset.elements,
                indexset.run__id,
                indexset.name,
                indexset.id,
                indexset.created_at,
                indexset.created_by,
            ]
            for indexset in indexsets
        ],
        columns=[
            "elements",
            "run__id",
            "name",
            "id",
            "created_at",
            "created_by",
        ],
    )


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
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements="foo"
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2]
        )

        indexset_1 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset 1"
        )
        indexset_2 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset 2"
        )
        expected = df_from_list(indexsets=[indexset_1, indexset_2])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.indexsets.tabulate()
        )

        expected = df_from_list(indexsets=[indexset_1])
        pdt.assert_frame_equal(
            expected,
            platform.backend.optimization.indexsets.tabulate(name="Indexset 1"),
        )
        # Test only indexsets belonging to this Run are tabulated if run_id is provided
        run_2 = platform.backend.runs.create("Model", "Scenario")
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, offset=2
        )
        expected = df_from_list(indexsets=[indexset_3, indexset_4])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.indexsets.tabulate(run_id=run_2.id)
        )

    def test_add_elements(self, platform: ixmp4.Platform):
        test_elements = ["foo", "bar"]
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id,
            elements=test_elements,  # type: ignore
        )
        indexset_1 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset 1"
        )

        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id,
            elements=test_elements,  # type: ignore
        )

        assert (
            indexset_1.elements
            == platform.backend.optimization.indexsets.get(
                run_id=run.id, name=indexset_2.name
            ).elements
        )

        with pytest.raises(ValueError):
            platform.backend.optimization.indexsets.add_elements(
                indexset_id=indexset_1.id, elements=["baz", "foo"]
            )

        with pytest.raises(ValueError):
            platform.backend.optimization.indexsets.add_elements(
                indexset_id=indexset_2.id, elements=["baz", "baz"]
            )

        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements=1
        )
        indexset_3 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_1.name
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements="1"
        )
        indexset_4 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_2.name
        )
        assert indexset_3.elements != indexset_4.elements
        assert len(indexset_3.elements) == len(indexset_4.elements)

        test_elements_2 = [1, "2", 3.14]
        indexset_5 = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 5"
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_5.id,
            elements=test_elements_2,  # type:ignore
        )
        indexset_5 = platform.backend.optimization.indexsets.get(
            run_id=run.id, name=indexset_5.name
        )
        assert indexset_5.elements == test_elements_2
