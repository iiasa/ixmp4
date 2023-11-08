import pytest

from ixmp4 import Scalar

from ..utils import all_platforms


@all_platforms
class TestDataOptimizationScalar:
    def test_create_scalar(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_id=unit.id
        )
        assert scalar.run__id == run.id
        assert scalar.name == "Scalar"
        assert scalar.value == 1
        assert scalar.unit__id == unit.id

    def test_get_scalar(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar_1 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_id=unit.id
        )
        # TODO
        # Check TODOs in the code and delete obsolete
        with pytest.raises(Scalar.NotUnique):
            _ = test_mp.backend.optimization.scalars.create(
                run_id=run.id, name="Scalar", value=2, unit_id=unit.id
            )

        scalar_2 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=2, unit_id=unit2.id
        )
        assert scalar_1 == test_mp.backend.optimization.scalars.get(
            run_id=run.id, name="Scalar", unit_id=unit.id
        )
        assert [scalar_1, scalar_2] == test_mp.backend.optimization.scalars.get(
            run_id=run.id, name="Scalar"
        )

    def test_list_scalars(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar_1 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_id=unit.id
        )
        scalar_2 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=2, unit_id=unit2.id
        )
        assert [scalar_1, scalar_2] == test_mp.backend.optimization.scalars.list()
