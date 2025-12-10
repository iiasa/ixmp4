import datetime

import pytest

import ixmp4
from tests import backends
from tests.custom_exception import CustomException

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class OptimizationScalarTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def unit(
        self,
        platform: ixmp4.Platform,
    ) -> ixmp4.Unit:
        unit = platform.units.create("Unit")
        assert unit.id == 1
        return unit


class TestScalar(OptimizationScalarTest):
    def test_create_scalar(
        self,
        run: ixmp4.Run,
        unit: ixmp4.Unit,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create scalars"):
            scalar1 = run.optimization.scalars.create("Scalar 1", 1.2, unit)
            scalar2 = run.optimization.scalars.create("Scalar 2", 2.3, None)
            scalar3 = run.optimization.scalars.create("Scalar 3", 3, unit.name)
            scalar4 = run.optimization.scalars.create("Scalar 4", 4)

        assert scalar1.id == 1
        assert scalar1.run_id == run.id
        assert scalar1.name == "Scalar 1"
        assert scalar1.value == 1.2
        assert scalar1.unit.name == "Unit"
        assert scalar1.created_by == "@unknown"
        assert scalar1.created_at == fake_time.replace(tzinfo=None)

        assert scalar2.id == 2
        assert scalar2.unit.name == ""

        assert scalar3.id == 3

        assert scalar4.id == 4

    def test_tabulate_scalar(self, run: ixmp4.Run) -> None:
        ret_df = run.optimization.scalars.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "value" in ret_df.columns
        assert "unit__id" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

        assert "run__id" not in ret_df.columns

    def test_list_scalar(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.scalars.list()) == 4

    def test_delete_scalar_via_func_obj(self, run: ixmp4.Run) -> None:
        with run.transact("Delete scalar 1"):
            scalar1 = run.optimization.scalars.get_by_name("Scalar 1")
            run.optimization.scalars.delete(scalar1)

    def test_delete_scalar_via_func_id(self, run: ixmp4.Run) -> None:
        with run.transact("Delete scalar 2"):
            run.optimization.scalars.delete(2)

    def test_delete_scalar_via_func_name(self, run: ixmp4.Run) -> None:
        with run.transact("Delete scalar 3"):
            run.optimization.scalars.delete("Scalar 3")

    def test_delete_scalar_via_obj(self, run: ixmp4.Run) -> None:
        scalar4 = run.optimization.scalars.get_by_name("Scalar 4")
        with run.transact("Delete scalar 4"):
            scalar4.delete()

    def test_scalar_empty(self, run: ixmp4.Run) -> None:
        assert run.optimization.scalars.tabulate().empty
        assert len(run.optimization.scalars.list()) == 0


class TestScalarUnique(OptimizationScalarTest):
    def test_scalar_unique(self, run: ixmp4.Run, unit: ixmp4.Unit) -> None:
        with run.transact("Scalar not unique"):
            run.optimization.scalars.create("Scalar", 1.2, unit.name)

            with pytest.raises(
                ixmp4.optimization.Scalar.NotUnique,
                match="Did you mean to call Scalar.update(...)?",
            ):
                run.optimization.scalars.create("Scalar", 3.142, unit.name)


class TestScalarNotFound(OptimizationScalarTest):
    def test_scalar_not_found(self, run: ixmp4.Run) -> None:
        with pytest.raises(ixmp4.optimization.Scalar.NotFound):
            run.optimization.scalars.get_by_name("Scalar")


class ScalarDataTest(OptimizationScalarTest):
    def test_scalar_add_data(
        self,
        run: ixmp4.Run,
        test_data: int | float,
        test_data_unit: ixmp4.Unit,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create scalar"):
            scalar = run.optimization.scalars.create(
                "Scalar", test_data, test_data_unit.name
            )

        assert scalar.value == test_data
        assert scalar.unit.id == test_data_unit.id
        assert scalar.unit.name == test_data_unit.name

    def test_scalar_update_unit(
        self,
        run: ixmp4.Run,
        test_data: int | float,
        test_data_update_unit: ixmp4.Unit,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Update scalar unit"):
            scalar = run.optimization.scalars.get_by_name("Scalar")
            scalar.update(unit_name=test_data_update_unit.name)

        assert scalar.value == test_data
        assert scalar.unit.id == test_data_update_unit.id
        assert scalar.unit.name == test_data_update_unit.name

    def test_scalar_update_full(
        self,
        run: ixmp4.Run,
        test_data_update: int | float,
        test_data_update_unit: ixmp4.Unit,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Update scalar full"):
            scalar = run.optimization.scalars.get_by_name("Scalar")
            scalar.update(test_data_update, test_data_update_unit.name)

        assert scalar.value == test_data_update
        assert scalar.unit.id == test_data_update_unit.id
        assert scalar.unit.name == test_data_update_unit.name


class TestScalarData(ScalarDataTest):
    @pytest.fixture(scope="class")
    def test_data(self) -> int | float:
        return 42.1337

    @pytest.fixture(scope="class")
    def test_data_unit(self, platform: ixmp4.Platform) -> ixmp4.Unit:
        return platform.units.create("Unit 1")

    @pytest.fixture(scope="class")
    def test_data_update(self) -> int | float:
        return -1

    @pytest.fixture(scope="class")
    def test_data_update_unit(self, platform: ixmp4.Platform) -> ixmp4.Unit:
        return platform.units.create("Unit 2")


class TestScalarRunLock(OptimizationScalarTest):
    def test_scalar_requires_lock(self, run: ixmp4.Run, unit: ixmp4.Unit) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.scalars.create("Scalar", 1)

        with run.transact("Create scalar"):
            scalar = run.optimization.scalars.create("Scalar", 1)

        with pytest.raises(ixmp4.Run.LockRequired):
            scalar.update(unit_name=unit.name)

        with pytest.raises(ixmp4.Run.LockRequired):
            scalar.update(value=2.1)

        with pytest.raises(ixmp4.Run.LockRequired):
            scalar.delete()

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.scalars.delete(scalar.id)


class TestScalarDocs(OptimizationScalarTest):
    def test_create_docs_via_func(self, run: ixmp4.Run, unit: ixmp4.Unit) -> None:
        with run.transact("Create scalar 1"):
            scalar1 = run.optimization.scalars.create("Scalar 1", 1.2, unit.name)

        scalar1_docs1 = run.optimization.scalars.set_docs(
            "Scalar 1", "Description of Scalar 1"
        )
        scalar1_docs2 = run.optimization.scalars.get_docs("Scalar 1")

        assert scalar1_docs1 == scalar1_docs2
        assert scalar1.docs == scalar1_docs1

    def test_create_docs_via_object(self, run: ixmp4.Run) -> None:
        with run.transact("Create scalar 2"):
            scalar2 = run.optimization.scalars.create("Scalar 2", 2.3)
        scalar2.docs = "Description of Scalar 2"

        assert run.optimization.scalars.get_docs("Scalar 2") == scalar2.docs

    def test_create_docs_via_setattr(self, run: ixmp4.Run) -> None:
        with run.transact("Create scalar 3"):
            scalar3 = run.optimization.scalars.create("Scalar 3", 3)
        setattr(scalar3, "docs", "Description of Scalar 3")

        assert run.optimization.scalars.get_docs("Scalar 3") == scalar3.docs

    def test_list_docs(self, run: ixmp4.Run) -> None:
        assert run.optimization.scalars.list_docs() == [
            "Description of Scalar 1",
            "Description of Scalar 2",
            "Description of Scalar 3",
        ]

        assert run.optimization.scalars.list_docs(id=3) == ["Description of Scalar 3"]

        assert run.optimization.scalars.list_docs(id__in=[1]) == [
            "Description of Scalar 1"
        ]

    def test_delete_docs_via_func(self, run: ixmp4.Run) -> None:
        scalar1 = run.optimization.scalars.get_by_name("Scalar 1")
        run.optimization.scalars.delete_docs("Scalar 1")
        scalar1 = run.optimization.scalars.get_by_name("Scalar 1")
        assert scalar1.docs is None

    def test_delete_docs_set_none(self, run: ixmp4.Run) -> None:
        scalar2 = run.optimization.scalars.get_by_name("Scalar 2")
        scalar2.docs = None
        scalar2 = run.optimization.scalars.get_by_name("Scalar 2")
        assert scalar2.docs is None

    def test_delete_docs_del(self, run: ixmp4.Run) -> None:
        scalar3 = run.optimization.scalars.get_by_name("Scalar 3")
        del scalar3.docs
        scalar3 = run.optimization.scalars.get_by_name("Scalar 3")
        assert scalar3.docs is None

    def test_docs_empty(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.scalars.list_docs()) == 0


class TestScalarRollback(OptimizationScalarTest):
    def test_scalar_update_failure(
        self,
        run: ixmp4.Run,
        unit: ixmp4.Unit,
    ) -> None:
        with run.transact("Create scalar"):
            scalar = run.optimization.scalars.create("Scalar", 1, unit)

        try:
            with run.transact("Update scalar value failure"):
                scalar.update(2.3)
                raise CustomException
        except CustomException:
            pass

    def test_scalar_versioning_after_update_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run, unit: ixmp4.Unit
    ) -> None:
        scalar = run.optimization.scalars.get_by_name("Scalar")
        assert scalar.value == 1
        assert scalar.unit.name == unit.name

    def test_scalar_non_versioning_after_update_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run, unit: ixmp4.Unit
    ) -> None:
        scalar = run.optimization.scalars.get_by_name("Scalar")
        assert scalar.value == 2.3
        assert scalar.unit.name == unit.name

    def test_scalar_docs_failure(self, run: ixmp4.Run) -> None:
        scalar = run.optimization.scalars.get_by_name("Scalar")

        try:
            with run.transact("Set scalar docs failure"):
                scalar.docs = "These docs should persist!"
                raise CustomException
        except CustomException:
            pass

    def test_scalar_after_docs_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        scalar = run.optimization.scalars.get_by_name("Scalar")
        assert scalar.docs == "These docs should persist!"

    def test_scalar_delete_failure(self, run: ixmp4.Run) -> None:
        scalar = run.optimization.scalars.get_by_name("Scalar")

        try:
            with run.transact("Delete scalar failure"):
                scalar.delete()
                raise CustomException
        except CustomException:
            pass

    def test_scalar_versioning_after_delete_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        scalar = run.optimization.scalars.get_by_name("Scalar")
        assert scalar.id == 1

    def test_scalar_non_versioning_after_delete_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        with pytest.raises(ixmp4.optimization.Scalar.NotFound):
            run.optimization.scalars.get_by_name("Scalar")
