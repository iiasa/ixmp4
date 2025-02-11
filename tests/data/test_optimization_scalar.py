import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.data.abstract import Scalar


def df_from_list(scalars: list[Scalar]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                scalar.run__id,
                scalar.value,
                scalar.unit.id,
                scalar.name,
                scalar.id,
                scalar.created_at,
                scalar.created_by,
            ]
            for scalar in scalars
        ],
        columns=[
            "run__id",
            "value",
            "unit__id",
            "name",
            "id",
            "created_at",
            "created_by",
        ],
    )


class TestDataOptimizationScalar:
    def test_create_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        unit2 = platform.backend.units.create("Unit 2")
        scalar = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name="Unit"
        )
        assert scalar.run__id == run.id
        assert scalar.name == "Scalar"
        assert scalar.value == 1
        assert scalar.unit__id == unit.id

        with pytest.raises(Scalar.NotUnique):
            _ = platform.backend.optimization.scalars.create(
                run_id=run.id, name="Scalar", value=2, unit_name=unit2.name
            )

    def test_delete_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        scalar_1 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=3.14, unit_name=unit.name
        )

        # Test unknown id raises
        with pytest.raises(Scalar.NotFound):
            platform.backend.optimization.scalars.delete(id=(scalar_1.id + 1))

        # TODO How to check that DeletionPrevented is raised?

        # Test normal deletion
        platform.backend.optimization.scalars.delete(id=scalar_1.id)

        assert platform.backend.optimization.scalars.tabulate().empty

    def test_get_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        scalar = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        assert scalar == platform.backend.optimization.scalars.get(
            run_id=run.id, name="Scalar"
        )

        with pytest.raises(Scalar.NotFound):
            _ = platform.backend.optimization.scalars.get(
                run_id=run.id, name="Scalar 2"
            )

    def test_update_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        unit2 = platform.backend.units.create("Unit 2")
        scalar = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        assert scalar.id == 1
        assert scalar.unit__id == unit.id

        ret = platform.backend.optimization.scalars.update(
            scalar.id, unit_id=unit2.id, value=20
        )

        assert ret.id == scalar.id == 1
        assert ret.unit__id == unit2.id
        assert ret.value == 20

    def test_list_scalars(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        unit2 = platform.backend.units.create("Unit 2")
        scalar_1 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        scalar_2 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 2", value=2, unit_name=unit2.name
        )
        assert [scalar_1] == platform.backend.optimization.scalars.list(name="Scalar")
        assert [scalar_1, scalar_2] == platform.backend.optimization.scalars.list()

    def test_tabulate_scalars(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        unit2 = platform.backend.units.create("Unit 2")
        scalar_1 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        scalar_2 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 2", value=2, unit_name=unit2.name
        )
        expected = df_from_list(scalars=[scalar_1, scalar_2])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.scalars.tabulate()
        )

        expected = df_from_list(scalars=[scalar_1])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.scalars.tabulate(name="Scalar")
        )

        # Test tabulation of scalars of particular run only
        run_2 = platform.backend.runs.create("Model", "Scenario")
        scalar_3 = platform.backend.optimization.scalars.create(
            run_id=run_2.id, name="Scalar", value=1, unit_name=unit.name
        )
        scalar_4 = platform.backend.optimization.scalars.create(
            run_id=run_2.id, name="Scalar 2", value=2, unit_name=unit2.name
        )
        expected = df_from_list(scalars=[scalar_3, scalar_4])
        pdt.assert_frame_equal(
            expected, platform.backend.optimization.scalars.tabulate(run_id=run_2.id)
        )
