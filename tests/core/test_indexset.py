import pandas as pd
import pandas.testing as pdt
import pytest

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
                indexset.run.id,
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
class TestCoreIndexSet:
    def test_create_indexset(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        indexset_1 = run.optimization.IndexSet("IndexSet 1")
        assert indexset_1.id == 1
        assert indexset_1.name == "IndexSet 1"

        indexset_2 = run.optimization.IndexSet("IndexSet 2")
        assert indexset_1.id != indexset_2.id

    def test_add_elements(self, test_mp):
        run = test_mp.runs.create("Model", "Scenario")
        test_elements = ["foo", "bar"]
        indexset_1 = run.optimization.IndexSet("IndexSet 1")
        indexset_1.add(test_elements)
        run.optimization.IndexSet("IndexSet 2").add(test_elements)
        indexset_2 = run.optimization.IndexSet("IndexSet 2")
        assert indexset_1.elements == indexset_2.elements

        with pytest.raises(ValueError):
            indexset_1.add(["baz", "foo"])

        with pytest.raises(ValueError):
            indexset_2.add(["baz", "baz"])

        indexset_1.add(1)
        indexset_3 = run.optimization.IndexSet("IndexSet 1")
        indexset_2.add("1")
        indexset_4 = run.optimization.IndexSet("IndexSet 2")
        assert indexset_3.elements != indexset_4.elements
        assert len(indexset_3.elements) == len(indexset_4.elements)

    def test_list_indexsets(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        # Per default, list() lists only `default` version runs:
        run.set_as_default()
        indexset_1 = run.optimization.IndexSet("Indexset 1")
        indexset_2 = run.optimization.IndexSet("Indexset 2")
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

    def test_tabulate_indexsets(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        # Per default, tabulate() lists only `default` version runs:
        run.set_as_default()
        indexset_1 = run.optimization.IndexSet("Indexset 1")
        indexset_2 = run.optimization.IndexSet("Indexset 2")
        exp = df_from_list(indexsets=[indexset_1, indexset_2])
        res = run.optimization.indexsets.tabulate()
        # utils.assert_unordered_equality doesn't like lists, so make sure the order in
        # df_from_list() is correct!
        pdt.assert_frame_equal(exp, res, check_dtype=False)

    def test_indexset_docs(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        indexset_1 = run.optimization.IndexSet("IndexSet 1")
        docs = "Documentation of IndexSet 1"
        indexset_1.docs = docs
        assert indexset_1.docs == docs

        indexset_1.docs = None
        assert indexset_1.docs is None
