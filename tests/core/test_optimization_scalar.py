import warnings
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Scalar
from ixmp4.core.exceptions import RunLockRequired

from .. import utils

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend


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

        # Test same name can be used again
        with run.transact("Test scalars.create() after delete"):
            scalar_1 = run.optimization.scalars.create(
                name="Scalar", value=2, unit=unit
            )

        assert scalar_1.name == "Scalar"
        assert scalar_1.value == 2

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
        utils.assert_unordered_equality(expected, result, check_dtype=False)

        expected = df_from_list(scalars=[scalar_2])
        result = run.optimization.scalars.tabulate(name="Scalar 2")
        utils.assert_unordered_equality(expected, result, check_dtype=False)

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

    def test_scalar_rollback_sqlite(self, sqlite_platform: ixmp4.Platform) -> None:
        run = sqlite_platform.runs.create("Model", "Scenario")
        unit = sqlite_platform.units.create("Test Unit")

        with run.transact("Test Scalar versioning"):
            scalar_1 = run.optimization.scalars.create(
                "Scalar 1", value=1, unit=unit.name
            )

        with warnings.catch_warnings(record=True) as w:
            try:
                with (
                    run.transact("Test Scalar versioning update on sqlite"),
                ):
                    scalar_1.value = 2
                    raise utils.CustomException("Whoops!!!")
            except utils.CustomException:
                pass

        scalar_1 = run.optimization.scalars.get(scalar_1.name)

        assert scalar_1.value == 2
        assert (
            "An exception occurred but the `Run` was not reverted because "
            "versioning is not supported by this platform" in str(w[0].message)
        )

    def test_versioning_scalar(self, pg_platform: ixmp4.Platform) -> None:
        run = pg_platform.runs.create("Model", "Scenario")
        unit = pg_platform.units.create("Test Unit")
        unit_2 = pg_platform.units.create("Unit 2")
        with run.transact("Test Scalar versioning"):
            scalar_1 = run.optimization.scalars.create(
                "Scalar 1", value=4, unit=unit.name
            )
            scalar_2 = run.optimization.scalars.create("Scalar 2", value=0)

        with run.transact("Test Scalar versioning with update"):
            scalar_1.value = 1.2
            scalar_1.unit = unit_2

        with run.transact("Test Scalar versioning with different transaction"):
            pg_platform.backend.optimization.scalars.update(
                scalar_2.id, value=3.14, unit_id=unit.id
            )

        @utils.versioning_test(pg_platform.backend)
        def assert_versions(backend: "SqlAlchemyBackend") -> None:
            vdf = backend.optimization.scalars.versions.tabulate()

            expected = pd.DataFrame(
                [
                    [
                        run.id,
                        4,
                        unit.id,
                        "Scalar 1",
                        1,
                        7,
                        12,
                        0,
                        scalar_1.created_at,
                        scalar_1.created_by,
                        "Test Unit",
                    ],
                    [
                        run.id,
                        0,
                        3,
                        "Scalar 2",
                        2,
                        9,
                        16,
                        0,
                        scalar_2.created_at,
                        scalar_2.created_by,
                        "",
                    ],
                    [
                        run.id,
                        1.2,
                        unit.id,
                        "Scalar 1",
                        1,
                        12,
                        13,
                        1,
                        scalar_1.created_at,
                        scalar_1.created_by,
                        "Test Unit",
                    ],
                    [
                        run.id,
                        1.2,
                        unit_2.id,
                        "Scalar 1",
                        1,
                        13,
                        None,
                        1,
                        scalar_1.created_at,
                        scalar_1.created_by,
                        "Unit 2",
                    ],
                    [
                        run.id,
                        3.14,
                        unit.id,
                        "Scalar 2",
                        2,
                        16,
                        None,
                        1,
                        scalar_2.created_at,
                        scalar_2.created_by,
                        "Test Unit",
                    ],
                ],
                columns=[
                    "run__id",
                    "value",
                    "unit__id",
                    "name",
                    "id",
                    "transaction_id",
                    "end_transaction_id",
                    "operation_type",
                    "created_at",
                    "created_by",
                    "unit",
                ],
            ).replace({np.nan: None})

            utils.assert_unordered_equality(expected, vdf, check_dtype=False)

    def test_scalar_rollback(self, pg_platform: ixmp4.Platform) -> None:
        run = pg_platform.runs.create("Model", "Scenario")
        unit = pg_platform.units.create("Test Unit")
        unit_2 = pg_platform.units.create("Unit 2")
        with run.transact("Test Scalar rollback"):
            scalar = run.optimization.scalars.create("Scalar 1", value=1.0, unit=unit)
            scalar_2 = run.optimization.scalars.create("Scalar 2", value=2, unit=unit_2)

        # Test rollback on two partial updates
        try:
            with run.transact("Test Scalar rollback partial failure"):
                scalar.value = 2.0
                scalar.unit = unit_2.name
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        # TODO Document this clearly: if you're working with facade objects and a
        # rollback is triggered, you need to reload the facade objects to reflect the
        # rolled-back data
        scalar = run.optimization.scalars.get("Scalar 1")
        assert scalar.value == 1.0
        assert scalar.unit.name == unit.name

        # Test rollback on one full update
        try:
            with run.transact("Test Scalar rollback full failure"):
                pg_platform.backend.optimization.scalars.update(
                    scalar.id, value=2.0, unit_id=unit_2.id
                )
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalar = run.optimization.scalars.get("Scalar 1")
        assert scalar.value == 1.0
        assert scalar.unit.name == unit.name

        # Test rollback on create
        try:
            with run.transact("Test Scalar rollback after create"):
                run.optimization.scalars.create("Scalar 3", value=3, unit=unit)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalars = run.optimization.scalars.tabulate()
        assert scalars["name"].to_list() == ["Scalar 1", "Scalar 2"]

        # Test rollback on delete
        try:
            with run.transact("Test Scalar rollback on delete"):
                run.optimization.scalars.delete("Scalar 2")
                print(run.optimization.scalars.tabulate().to_string())
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalars = run.optimization.scalars.tabulate()
        assert scalars["name"].to_list() == ["Scalar 1", "Scalar 2"]

        # Test rollback to deleted unit
        try:
            with run.transact(
                "Test Scalar rollback to deleted Unit", revert_platform_on_error=True
            ):
                scalar.unit = unit_2
                pg_platform.units.delete(unit)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalar = run.optimization.scalars.get("Scalar 1")
        assert scalar.unit.name == unit.name

        # Test rollback after (implicit) creation of new unit
        try:
            with run.transact("Test Scalar rollback after unit creation"):
                run.optimization.scalars.create("Scalar 3", value=3)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass
        # Nothing to assert here, just checking that the rollback succeeds without error

        # Test rollback with potential id-reuse
        try:
            with run.transact("Test Scalar rollback with id-reuse"):
                run.optimization.scalars.delete("Scalar 2")
                run.optimization.scalars.create("Scalar 3", value=3)
                raise utils.CustomException
        except utils.CustomException:
            pass

        scalar_2 = run.optimization.scalars.get("Scalar 2")
        assert scalar_2.value == 2
        assert scalar_2.unit.name == unit_2.name

        # Test rollback with multiple creations and deletes with the same name
        try:
            with run.transact("Test Scalar rollback with multiple re-uses"):
                run.optimization.scalars.create("Scalar 3", value=3)
                run.optimization.scalars.delete("Scalar 3")
                scalar_3 = run.optimization.scalars.create("Scalar 3", value=3)
                scalar_3.value = 6
                run.optimization.scalars.delete("Scalar 3")
                run.optimization.scalars.create("Scalar 3", value=3)
                run.optimization.scalars.delete("Scalar 3")
                run.optimization.scalars.create("Scalar 4", value=4)
                run.optimization.scalars.create("Scalar 3", value=3)
                run.optimization.scalars.delete("Scalar 3")
                run.optimization.scalars.create("Scalar 3", value=3)
                run.optimization.scalars.delete("Scalar 3")
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        # Nothing to assert here, really, just checking the above runs without error
        assert len(run.optimization.scalars.tabulate()) == 2

    def test_scalar_rollback_to_checkpoint(self, pg_platform: ixmp4.Platform) -> None:
        run = pg_platform.runs.create("Model", "Scenario")
        unit = pg_platform.units.create("Test Unit")
        unit_2 = pg_platform.units.create("Unit 2")

        try:
            with run.transact("Test Scalar rollback to checkpoint"):
                scalar = run.optimization.scalars.create(
                    "Scalar 1", value=1.0, unit=unit
                )
                run.checkpoints.create("Test Scalar rollback")
                scalar.value = 2.0
                scalar.unit = unit_2.name
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalar = run.optimization.scalars.get("Scalar 1")
        assert scalar.value == 1.0
        assert scalar.unit.name == unit.name
        assert len(run.checkpoints.tabulate()) == 1

        # Test rollback on delete (even when an unrelated delete transaction exists)
        try:
            with run.transact("Test Scalar rollback on delete"):
                scalar_2 = run.optimization.scalars.create(
                    "Scalar 2", value=2, unit=unit
                )
                run.optimization.scalars.delete(scalar_2.id)
                run.optimization.scalars.create("Scalar 3", value=3, unit=unit_2)
                run.checkpoints.create("Test Scalar rollback with existing delete")
                run.optimization.scalars.delete(scalar.id)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalar = run.optimization.scalars.get("Scalar 1")
        assert scalar.value == 1.0
        assert scalar.unit.name == unit.name

        # Test rollback to deleted Scalar after creation of one with the same name
        try:
            with run.transact("Test Scalar rollback to deleted Scalar"):
                scalar_2 = run.optimization.scalars.create(
                    "Scalar 2", value=2, unit=unit_2
                )
                run.checkpoints.create("Test Scalar rollback to deleted Scalar")
                run.optimization.scalars.delete(scalar_2.id)
                run.optimization.scalars.create("Scalar 2", value=3, unit=unit_2)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalar_2 = run.optimization.scalars.get("Scalar 2")
        assert scalar_2.value == 2

        # Test rollback to overwritten id
        # NOTE This will only happen on sqlite (and not even there if we configure
        # AUTOINCREMENT as on pgsql)
        try:
            with run.transact("Test Scalar rollback to overwritten id"):
                run.optimization.scalars.create("Scalar 4", value=4, unit=unit)
                run.checkpoints.create("Test Scalar rollback to overwritten id")
                run.optimization.scalars.delete("Scalar 4")
                run.optimization.scalars.create("Scalar 5", value=5, unit=unit)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        scalar_list = run.optimization.scalars.list()
        assert len(scalar_list) == 4
        assert scalar_list[3].name == "Scalar 4"
