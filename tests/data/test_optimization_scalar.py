import pytest

from ixmp4 import Scalar

from ..utils import all_platforms


@all_platforms
class TestDataOptimizationScalar:
    def test_create_scalar(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name="Unit"
        )
        assert scalar.run__id == run.id
        assert scalar.name == "Scalar"
        assert scalar.value == 1
        assert scalar.unit__id == unit.id

        with pytest.raises(Scalar.NotUnique):
            _ = test_mp.backend.optimization.scalars.create(
                run_id=run.id, name="Scalar", value=2, unit_name=unit2.name
            )

    def test_get_scalar(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )

        assert scalar == test_mp.backend.optimization.scalars.get(
            run_id=run.id, name="Scalar"
        )

    def test_update_scalar(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        assert scalar.id == 1
        assert scalar.unit__id == unit.id

        scalar = test_mp.backend.optimization.scalars.update(
            "Scalar", value=20, unit_name="Unit 2", run_id=run.id
        )
        assert scalar.value == 20
        assert scalar.unit__id == unit2.id

    def test_list_scalars(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar_1 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        scalar_2 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 2", value=2, unit_name=unit2.name
        )
        assert [scalar_1, scalar_2] == test_mp.backend.optimization.scalars.list()