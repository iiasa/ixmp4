import pytest

from ixmp4 import IndexSet

from ..utils import all_platforms


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

    def test_get_indexset(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        _ = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )

        with pytest.raises(IndexSet.NotUnique):
            _ = test_mp.backend.optimization.indexsets.create(
                run_id=run.id, name="Indexset"
            )

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
        assert [indexset_1, indexset_2] == test_mp.backend.optimization.indexsets.list()

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
