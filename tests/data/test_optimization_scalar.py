import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4 import Scalar

from ..utils import all_platforms


def df_from_list(scalars: list):
    return pd.DataFrame(
        [
            [
                scalar.name,
                scalar.value,
                scalar.unit.id,
                scalar.run__id,
                scalar.created_at,
                scalar.created_by,
                scalar.id,
            ]
            for scalar in scalars
        ],
        columns=[
            "name",
            "value",
            "unit__id",
            "run__id",
            "created_at",
            "created_by",
            "id",
        ],
    )


@all_platforms
class TestDataOptimizationScalar:
    def test_create_scalar(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
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

    def test_get_scalar(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        assert scalar == test_mp.backend.optimization.scalars.get(
            run_id=run.id, name="Scalar"
        )

        with pytest.raises(Scalar.NotFound):
            _ = test_mp.backend.optimization.scalars.get(run_id=run.id, name="Scalar 2")

    def test_update_scalar(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        assert scalar.id == 1
        assert scalar.unit__id == unit.id

        ret = test_mp.backend.optimization.scalars.update(
            scalar.id, unit_id=unit2.id, value=20
        )

        assert ret.id == scalar.id == 1
        assert ret.unit__id == unit2.id
        assert ret.value == 20

    def test_list_scalars(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
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
        assert [scalar_1] == test_mp.backend.optimization.scalars.list(name="Scalar")
        assert [scalar_1, scalar_2] == test_mp.backend.optimization.scalars.list()

    def test_tabulate_scalars(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, tabulate() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        unit = test_mp.backend.units.create("Unit")
        unit2 = test_mp.backend.units.create("Unit 2")
        scalar_1 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        scalar_2 = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 2", value=2, unit_name=unit2.name
        )
        expected = df_from_list(scalars=[scalar_1, scalar_2])
        pdt.assert_frame_equal(
            expected, test_mp.backend.optimization.scalars.tabulate()
        )

        expected = df_from_list(scalars=[scalar_1])
        pdt.assert_frame_equal(
            expected, test_mp.backend.optimization.scalars.tabulate(name="Scalar")
        )
