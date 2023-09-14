import pytest

from ..utils import all_platforms


@all_platforms
class TestCoreIndexSet:
    def test_create_indexset(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        index_set_1 = run.optimization.IndexSet("IndexSet 1")
        returned_index_set_1 = run.optimization.IndexSet("IndexSet 1")
        assert index_set_1.id == returned_index_set_1.id

        index_set_2 = run.optimization.IndexSet("IndexSet 2")
        assert index_set_1.id != index_set_2.id

    def test_add_elements(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        test_elements = ["foo", "bar"]
        index_set_1 = run.optimization.IndexSet("IndexSet 1")
        index_set_1.add(test_elements)
        run.optimization.IndexSet("IndexSet 2").add(test_elements)
        index_set_2 = run.optimization.IndexSet("IndexSet 2")
        assert index_set_1.elements == index_set_2.elements

        with pytest.raises(ValueError):
            index_set_1.add(["baz", "foo"])

        with pytest.raises(ValueError):
            index_set_2.add(["baz", "baz"])

        index_set_1.add(1)
        index_set_3 = run.optimization.IndexSet("IndexSet 1")
        index_set_2.add("1")
        index_set_4 = run.optimization.IndexSet("IndexSet 2")
        assert index_set_3.elements != index_set_4.elements
        assert len(index_set_3.elements) == len(index_set_4.elements)

    def test_indexset_docs(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        index_set_1 = run.optimization.IndexSet("IndexSet 1")
        docs = "Documentation of IndexSet 1"
        index_set_1.docs = docs
        assert index_set_1.docs == docs

        index_set_1.docs = None
        assert index_set_1.docs is None
