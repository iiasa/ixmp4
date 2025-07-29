import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Scalar
from ixmp4.core.exceptions import RunLockRequired

from ..utils import assert_unordered_equality


def df_from_list(scalars: list[Scalar]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                scalar.id,
                scalar.name,
                scalar.value,
                scalar.unit.id,
                scalar.run_id,
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


class TestCoreScalar:
    def test_create_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Test Unit")
        with run.transact("Test scalars.create()"):
            scalar_1 = run.optimization.scalars.create(
                "Scalar 1", value=10, unit="Test Unit"
            )
        assert scalar_1.id == 1
        assert scalar_1.name == "Scalar 1"
        assert scalar_1.value == 10
        # Why is this raising an AssertionError?
        # assert scalar_1.unit == unit
        assert scalar_1.unit.id == unit.id

        # Test create without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.scalars.create("Scalar 2", value=20)

        unit2 = platform.units.create("Test Unit 2")
        with run.transact("Test scalars.create() errors"):
            with pytest.raises(Scalar.NotUnique):
                # Test creation with a different value
                scalar_2 = run.optimization.scalars.create(
                    "Scalar 1", value=20, unit=unit.name
                )

            with pytest.raises(Scalar.NotUnique):
                # Test creation with a different unit
                _ = run.optimization.scalars.create(
                    "Scalar 1", value=20, unit=unit2.name
                )

            with pytest.raises(TypeError):
                # Testing a missing parameter on purpose
                _ = run.optimization.scalars.create("Scalar 2")  # type: ignore[call-arg]

            scalar_2 = run.optimization.scalars.create("Scalar 2", value=20, unit=unit)
        assert scalar_1.id != scalar_2.id

        with run.transact("Test scalars.create() dimensionless"):
            scalar_3 = run.optimization.scalars.create("Scalar 3", value=1)
        assert scalar_3.unit.name == ""

    def test_delete_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Unit")
        with run.transact("Test scalars.delete()"):
            scalar_1 = run.optimization.scalars.create(
                name="Scalar", value=3.14, unit=unit
            )

            # Test unknown name raises
            with pytest.raises(Scalar.NotFound):
                run.optimization.scalars.delete(item="does not exist")

            # TODO How to check that DeletionPrevented is raised?

            # Test normal deletion
            run.optimization.scalars.delete(item=scalar_1.name)

        assert run.optimization.scalars.tabulate().empty

        # Test delete without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.scalars.delete(item=1)

    def test_get_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Test Unit")
        with run.transact("Test scalars.get()"):
            scalar = run.optimization.scalars.create("Scalar", value=10, unit=unit.name)
        result = run.optimization.scalars.get(scalar.name)
        assert scalar.id == result.id
        assert scalar.name == result.name
        assert scalar.value == result.value
        assert scalar.unit.id == result.unit.id

        with pytest.raises(Scalar.NotFound):
            _ = run.optimization.scalars.get("Foo")

    def test_update_scalar(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Test Unit")
        with run.transact("Test Scalar update() -- preparation"):
            scalar = run.optimization.scalars.create("Scalar", value=10, unit=unit.name)
        assert scalar.value == 10
        assert scalar.unit.id == unit.id

        with run.transact("Test Scalar update()"):
            scalar.value = 3.0
            scalar.unit = "Test Unit"
        # NOTE: doesn't work for some reason (but doesn't either for e.g. model.get())
        # assert scalar == run.optimization.scalars.get("Scalar")
        result = run.optimization.scalars.get("Scalar")

        assert scalar.id == result.id
        assert scalar.name == result.name
        assert scalar.value == result.value == 3.0
        assert scalar.unit.id == result.unit.id == 1

        # Test update without run lock raises
        with pytest.raises(RunLockRequired):
            scalar.value = 1
        with pytest.raises(RunLockRequired):
            scalar.unit = "1"

    def test_list_scalars(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Test Unit")
        with run.transact("Test scalars.list()"):
            scalar_1 = run.optimization.scalars.create(
                "Scalar 1", value=1, unit="Test Unit"
            )
            scalar_2 = run.optimization.scalars.create(
                "Scalar 2", value=2, unit=unit.name
            )

        # Create scalar in another run to test listing scalars for specific run
        run_2 = platform.runs.create("Model 2", "Scenario 2")
        with run_2.transact("Test scalars.list() 2"):
            run_2.optimization.scalars.create("Scalar 1", value=1, unit=unit)

        expected_ids = [scalar_1.id, scalar_2.id]
        list_ids = [scalar.id for scalar in run.optimization.scalars.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [scalar_1.id]
        list_id = [
            scalar.id for scalar in run.optimization.scalars.list(name="Scalar 1")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_scalars(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Test Unit")
        with run.transact("Test scalars.tabulate()"):
            scalar_1 = run.optimization.scalars.create(
                "Scalar 1", value=1, unit=unit.name
            )
            scalar_2 = run.optimization.scalars.create(
                "Scalar 2", value=2, unit=unit.name
            )

        # Create scalar in another run to test tabulating scalars for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        with run_2.transact("Test scalars.tabulate() 2"):
            run_2.optimization.scalars.create("Scalar 1", value=1, unit=unit)

        expected = df_from_list(scalars=[scalar_1, scalar_2])
        result = run.optimization.scalars.tabulate()
        assert_unordered_equality(expected, result, check_dtype=False)

        expected = df_from_list(scalars=[scalar_2])
        result = run.optimization.scalars.tabulate(name="Scalar 2")
        assert_unordered_equality(expected, result, check_dtype=False)

    def test_scalar_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Test Unit")
        with run.transact("Test Scalar.docs"):
            scalar = run.optimization.scalars.create(
                "Scalar 1", value=4, unit=unit.name
            )
        docs = "Documentation of Scalar 1"
        scalar.docs = docs
        assert scalar.docs == docs

        scalar.docs = None
        assert scalar.docs is None
