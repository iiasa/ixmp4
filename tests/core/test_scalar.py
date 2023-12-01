import pandas as pd
import pytest

from ixmp4 import Scalar

from ..utils import all_platforms, assert_unordered_equality


def df_from_list(scalars: list):
    return pd.DataFrame(
        [
            [
                scalar.id,
                scalar.name,
                scalar.value,
                scalar.unit.id,
                scalar.run.id,
                scalar.created_at,
                scalar.created_by,
            ]
            for scalar in scalars
        ],
        columns=[
            "id",
            "name",
            "value",
            "unit__id",
            "run__id",
            "created_at",
            "created_by",
        ],
    )


@all_platforms
class TestCoreScalar:
    def test_create_scalar(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        unit = test_mp.units.create("Test Unit")
        scalar_1 = run.optimization.Scalar("Scalar 1", value=10, unit_name="Test Unit")
        assert scalar_1.id == 1
        assert scalar_1.name == "Scalar 1"
        assert scalar_1.value == 10
        # Why is this raising an AssertionError?
        # assert scalar_1.unit == unit
        assert scalar_1.unit.id == unit.id

        with pytest.raises(Scalar.NotUnique):
            scalar_2 = run.optimization.scalars.create(
                "Scalar 1", value=20, unit_name=unit.name
            )

        with pytest.raises(TypeError):
            _ = run.optimization.Scalar("Scalar 2")

        scalar_2 = run.optimization.scalars.create(
            "Scalar 2", value=20, unit_name=unit.name
        )
        assert scalar_1.id != scalar_2.id

    def test_get_scalar(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        unit = test_mp.units.create("Test Unit")
        scalar = run.optimization.scalars.create(
            "Scalar", value=10, unit_name=unit.name
        )
        result = run.optimization.scalars.get(scalar.name)
        assert scalar.id == result.id
        assert scalar.name == result.name
        assert scalar.value == result.value
        assert scalar.unit.id == result.unit.id

    def test_update_scalar(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        unit = test_mp.units.create("Test Unit")
        unit2 = test_mp.units.create("Test Unit 2")
        scalar = run.optimization.scalars.create(
            "Scalar", value=10, unit_name=unit.name
        )
        assert scalar.value == 10
        assert scalar.unit.id == unit.id

        with pytest.raises(Scalar.NotUnique):
            _ = run.optimization.scalars.create(
                "Scalar", value=20, unit_name=unit2.name
            )

        scalar = run.optimization.scalars.update(
            "Scalar", value=20, unit_name=unit2.name
        )
        assert scalar.value == 20
        assert scalar.unit.id == unit2.id

        scalar.value = 30
        scalar.unit = "Test Unit"
        # NOTE: doesn't work for some reason (but doesn't either for e.g. model.get())
        # assert scalar == run.optimization.scalars.get("Scalar")
        result = run.optimization.scalars.get("Scalar")
        print(scalar.value, scalar.unit)
        print(result.value, result.unit)
        assert scalar.id == result.id
        assert scalar.name == result.name
        assert scalar.value == result.value == 30
        assert scalar.unit.id == result.unit.id == 1

    def test_list_scalars(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        # Per default, list() lists only `default` version runs:
        run.set_as_default()
        unit = test_mp.units.create("Test Unit")
        scalar_1 = run.optimization.Scalar("Scalar 1", value=1, unit_name="Test Unit")
        scalar_2 = run.optimization.Scalar("Scalar 2", value=2, unit_name=unit.name)
        expected_ids = [scalar_1.id, scalar_2.id]
        list_ids = [scalar.id for scalar in run.optimization.scalars.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [scalar_1.id]
        list_id = [
            scalar.id for scalar in run.optimization.scalars.list(name="Scalar 1")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_scalars(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        # Per default, tabulate() lists only `default` version runs:
        run.set_as_default()
        unit = test_mp.units.create("Test Unit")
        scalar_1 = run.optimization.Scalar("Scalar 1", value=1, unit_name=unit.name)
        scalar_2 = run.optimization.Scalar("Scalar 2", value=2, unit_name=unit.name)
        expected = df_from_list(scalars=[scalar_1, scalar_2])
        result = run.optimization.scalars.tabulate()
        assert_unordered_equality(expected, result, check_dtype=False)

        expected = df_from_list(scalars=[scalar_2])
        result = run.optimization.scalars.tabulate(name="Scalar 2")
        assert_unordered_equality(expected, result, check_dtype=False)

    def test_scalar_docs(self, test_mp):
        run = test_mp.Run("Model", "Scenario", "new")
        unit = test_mp.units.create("Test Unit")
        scalar = run.optimization.Scalar("Scalar 1", value=4, unit_name=unit.name)
        docs = "Documentation of Scalar 1"
        scalar.docs = docs
        assert scalar.docs == docs

        scalar.docs = None
        assert scalar.docs is None
