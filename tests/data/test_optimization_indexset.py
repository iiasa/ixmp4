import pytest

from ..utils import all_platforms


@all_platforms
class TestDataOptimizationIndexSet:
    def test_create_indexset(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        index_set_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        assert index_set_1 == test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="Indexset"
        )

    def test_add_elements(self, test_mp):
        test_elements = ["foo", "bar"]
        run = test_mp.backend.runs.create("Model", "Scenario")
        index_set_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 1"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=index_set_1.id, elements=test_elements
        )
        index_set_1 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 1"
        )

        index_set_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 2"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=index_set_2.id, elements=test_elements
        )

        assert (
            index_set_1.elements
            == test_mp.backend.optimization.indexsets.get(
                run_id=run.id, name="IndexSet 2"
            ).elements
        )

        with pytest.raises(ValueError):
            test_mp.backend.optimization.indexsets.add_elements(
                indexset_id=index_set_1.id, elements=["baz", "foo"]
            )

        with pytest.raises(ValueError):
            test_mp.backend.optimization.indexsets.add_elements(
                indexset_id=index_set_2.id, elements=["baz", "baz"]
            )

        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=index_set_1.id, elements=1
        )
        index_set_3 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 1"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=index_set_2.id, elements="1"
        )
        index_set_4 = test_mp.backend.optimization.indexsets.get(
            run_id=run.id, name="IndexSet 2"
        )
        assert index_set_3.elements != index_set_4.elements
        assert len(index_set_3.elements) == len(index_set_4.elements)
