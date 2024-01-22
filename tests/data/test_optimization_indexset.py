import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4 import IndexSet

from ..utils import all_platforms


def df_from_list(indexsets: list):
    return pd.DataFrame(
        # Order is important here to avoid utils.assert_unordered_equality,
        # which doesn't like lists
        [
            [
                indexset.name,
                indexset.elements,
                indexset.created_at,
                indexset.created_by,
                indexset.run__id,
                indexset.id,
            ]
            for indexset in indexsets
        ],
        columns=[
            "name",
            "elements",
            "created_at",
            "created_by",
            "run__id",
            "id",
        ],
    )


@all_platforms
class TestDataOptimizationIndexSet:
    def test_create_indexset(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        assert indexset_1.id == 1
        assert indexset_1.run__id == 1
        assert indexset_1.name == "Indexset"

        with pytest.raises(IndexSet.NotUnique):
            _ = test_mp.backend.optimization.indexsets.create(
                run_id=run.id, name="Indexset"
            )

    def test_get_indexset(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        _ = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        indexset = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset"
        )

        assert indexset.id == 1
        assert indexset.run__id == 1
        assert indexset.name == "Indexset"

    def test_list_indexsets(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 1"
        )
        indexset_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 2"
        )
        assert [indexset_1] == test_mp.backend.optimization.indexsets.list(
            name="Indexset 1"
        )
        assert [indexset_1, indexset_2] == test_mp.backend.optimization.indexsets.list()

    def test_tabulate_indexsets(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, tabulate() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 1"
        )
        indexset_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 2"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements="foo"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2]
        )

        indexset_1 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset 1"
        )
        indexset_2 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset 2"
        )
        expected = df_from_list(indexsets=[indexset_1, indexset_2])
        pdt.assert_frame_equal(
            expected, test_mp.backend.optimization.indexsets.tabulate()
        )

        expected = df_from_list(indexsets=[indexset_1])
        pdt.assert_frame_equal(
            expected, test_mp.backend.optimization.indexsets.tabulate(name="Indexset 1")
        )

    def test_add_elements(self, test_mp):
        test_elements = ["foo", "bar"]
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 1"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements=test_elements
        )
        indexset_1 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 1"
        )

        indexset_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 2"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=test_elements
        )

        assert (
            indexset_1.elements
            == test_mp.backend.optimization.indexsets.get(
                run_id=run.id, name="IndexSet 2"
            ).elements
        )

        with pytest.raises(ValueError):
            test_mp.backend.optimization.indexsets.add_elements(
                indexset_id=indexset_1.id, elements=["baz", "foo"]
            )

        with pytest.raises(ValueError):
            test_mp.backend.optimization.indexsets.add_elements(
                indexset_id=indexset_2.id, elements=["baz", "baz"]
            )

        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements=1
        )
        indexset_3 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 1"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements="1"
        )
        indexset_4 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 2"
        )
        assert indexset_3.elements != indexset_4.elements
        assert len(indexset_3.elements) == len(indexset_4.elements)
