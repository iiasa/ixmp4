import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.core import IndexSet, Platform

from ..utils import all_platforms


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


@all_platforms
class TestDataOptimizationIndexSet:
    def test_create_indexset(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
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

    def test_get_indexset(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
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

        with pytest.raises(IndexSet.NotFound):
            _ = test_mp.backend.optimization.indexsets.get(
                run_id=run.id, name="Indexset 2"
            )

    def test_list_indexsets(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
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

        # Test only indexsets belonging to this Run are listed when run_id is provided
        run_2 = test_mp.backend.runs.create("Model", "Scenario")
        indexset_3 = test_mp.backend.optimization.indexsets.create(
            run_id=run_2.id, name="Indexset 1"
        )
        indexset_4 = test_mp.backend.optimization.indexsets.create(
            run_id=run_2.id, name="Indexset 2"
        )
        assert [indexset_3, indexset_4] == test_mp.backend.optimization.indexsets.list(
            run_id=run_2.id
        )

    def test_tabulate_indexsets(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
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

        # Test only indexsets belonging to this Run are tabulated if run_id is provided
        run_2 = test_mp.backend.runs.create("Model", "Scenario")
        indexset_3 = test_mp.backend.optimization.indexsets.create(
            run_id=run_2.id, name="Indexset 1"
        )
        indexset_4 = test_mp.backend.optimization.indexsets.create(
            run_id=run_2.id, name="Indexset 2"
        )
        expected = df_from_list(indexsets=[indexset_3, indexset_4])
        pdt.assert_frame_equal(
            expected, test_mp.backend.optimization.indexsets.tabulate(run_id=run_2.id)
        )

    def test_add_elements(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
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

        test_elements_2 = [1, "2", 3.14]
        indexset_5 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 5"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_5.id, elements=test_elements_2
        )
        indexset_5 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 5"
        )
        assert indexset_5.elements == test_elements_2
