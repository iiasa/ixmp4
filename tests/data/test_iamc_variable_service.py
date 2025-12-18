import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.data.iamc.variable.exceptions import VariableNotFound, VariableNotUnique
from ixmp4.data.iamc.variable.service import VariableService
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import Transport
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class VariableServiceTest(ServiceTest[VariableService]):
    service_class = VariableService


class TestVariableCreateViaTimeseries(VariableServiceTest):
    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def run(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Variable", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def units(self, transport: Transport) -> UnitService:
        return UnitService(transport)

    @pytest.fixture(scope="class")
    def regions(self, transport: Transport) -> RegionService:
        return RegionService(transport)

    @pytest.fixture(scope="class")
    def timeseries(self, transport: Transport) -> TimeSeriesService:
        return TimeSeriesService(transport)

    def create_related(
        self,
        regions: RegionService,
        units: UnitService,
    ) -> None:
        # assume regions and units have been created
        # by a manager
        regions.create("Region 1", "default")
        regions.create("Region 2", "default")
        units.create("Unit 1")
        units.create("Unit 2")

    @pytest.fixture(scope="class")
    def test_ts_df(
        self,
        run: Run,
        regions: RegionService,
        units: UnitService,
        fake_time: datetime.datetime,
    ) -> pd.DataFrame:
        self.create_related(regions, units)
        return pd.DataFrame(
            [
                [run.id, "Region 1", "Variable 1", "Unit 1"],
                [run.id, "Region 1", "Variable 2", "Unit 2"],
                [run.id, "Region 2", "Variable 1", "Unit 1"],
            ],
            columns=["run__id", "region", "variable", "unit"],
        )

    @pytest.fixture(scope="class")
    def test_df_expected(self, run: Run, fake_time: datetime.datetime) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, "Variable 1", fake_time.replace(tzinfo=None), "@unknown"],
                [2, "Variable 2", fake_time.replace(tzinfo=None), "@unknown"],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

    def test_variable_tabulate(
        self,
        service: VariableService,
        timeseries: TimeSeriesService,
        test_ts_df: pd.DataFrame,
        test_df_expected: pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        timeseries.bulk_upsert(test_ts_df)
        ret_df = service.tabulate()
        pdt.assert_frame_equal(test_df_expected, ret_df, check_like=True)


class TestVariableCreate(VariableServiceTest):
    def test_variable_create(
        self, service: VariableService, fake_time: datetime.datetime
    ) -> None:
        variable = service.create("Variable")
        assert variable.name == "Variable"
        assert variable.created_at == fake_time.replace(tzinfo=None)
        assert variable.created_by == "@unknown"

    def test_variable_create_versioning(
        self, versioning_service: VariableService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Variable",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestVariableDeleteById(VariableServiceTest):
    def test_variable_delete_by_id(
        self, service: VariableService, fake_time: datetime.datetime
    ) -> None:
        variable = service.create("Variable")
        service.delete_by_id(variable.id)
        assert service.tabulate().empty

    def test_variable_delete_by_id_versioning(
        self, versioning_service: VariableService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Variable",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    2,
                    0,
                ],
                [
                    1,
                    "Variable",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    2,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )


class TestVariableUnique(VariableServiceTest):
    def test_variable_unique(self, service: VariableService) -> None:
        service.create("Variable")

        with pytest.raises(VariableNotUnique):
            service.create("Variable")


class TestVariableGetByName(VariableServiceTest):
    def test_variable_get_by_name(self, service: VariableService) -> None:
        variable1 = service.create("Variable")
        variable2 = service.get_by_name("Variable")
        assert variable1 == variable2


class TestVariableGetById(VariableServiceTest):
    def test_variable_get_by_id(self, service: VariableService) -> None:
        variable1 = service.create("Variable")
        variable2 = service.get_by_id(1)
        assert variable1 == variable2


class TestVariableNotFound(VariableServiceTest):
    def test_variable_not_found(self, service: VariableService) -> None:
        with pytest.raises(VariableNotFound):
            service.get_by_name("Variable")

        with pytest.raises(VariableNotFound):
            service.get_by_id(1)


class TestVariableList(VariableServiceTest):
    def test_variable_list(
        self, service: VariableService, fake_time: datetime.datetime
    ) -> None:
        service.create("Variable 1")
        service.create("Variable 2")

        variables = service.list()

        assert variables[0].id == 1
        assert variables[0].name == "Variable 1"
        assert variables[0].created_by == "@unknown"
        assert variables[0].created_at == fake_time.replace(tzinfo=None)

        assert variables[1].id == 2
        assert variables[1].name == "Variable 2"
        assert variables[1].created_by == "@unknown"
        assert variables[1].created_at == fake_time.replace(tzinfo=None)


class TestVariableTabulate(VariableServiceTest):
    def test_variable_tabulate(
        self, service: VariableService, fake_time: datetime.datetime
    ) -> None:
        service.create("Variable 1")
        service.create("Variable 2")

        expected_variables = pd.DataFrame(
            [
                [
                    1,
                    "Variable 1",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "Variable 2",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

        variables = service.tabulate()
        pdt.assert_frame_equal(variables, expected_variables, check_like=True)


class TestVariableAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, VariableServiceTest
):
    def test_variable_create(self, service: VariableService) -> None:
        variable = service.create("Variable")
        assert variable.id == 1
        assert variable.created_by == "superuser_sarah"

    def test_variable_get_by_name(self, service: VariableService) -> None:
        variable = service.get_by_name("Variable")
        assert variable.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        variable = service.get_by_id(1)
        assert variable.name == "Variable"

    def test_variable_list(self, service: VariableService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        service.delete_by_id(1)


class TestVariableAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, VariableServiceTest
):
    def test_variable_create(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            variable = service.create("Variable")
            assert variable.id == 1

    def test_variable_get_by_name(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_name("Variable")

    def test_variable_get_by_id(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_variable_list(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_variable_tabulate(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_variable_delete(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestVariableAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, VariableServiceTest
):
    def test_variable_create(self, service: VariableService) -> None:
        variable = service.create("Variable")
        assert variable.id == 1
        assert variable.created_by == "staffuser_bob"

    def test_variable_get_by_name(self, service: VariableService) -> None:
        variable = service.get_by_name("Variable")
        assert variable.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        variable = service.get_by_id(1)
        assert variable.name == "Variable"

    def test_variable_list(self, service: VariableService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        service.delete_by_id(1)


class TestVariableAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, VariableServiceTest
):
    def test_variable_create(
        self, service: VariableService, unauthorized_service: VariableService
    ) -> None:
        with pytest.raises(Forbidden):
            variable = service.create("Variable")
            assert variable.id == 1
        unauthorized_service.create("Variable")

    def test_variable_get_by_name(self, service: VariableService) -> None:
        variable = service.get_by_name("Variable")
        assert variable.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        variable = service.get_by_id(1)
        assert variable.name == "Variable"

    def test_variable_list(self, service: VariableService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestVariableAuthNonePrivate(
    auth.NoneTest, auth.PrivatePlatformTest, VariableServiceTest
):
    def test_variable_create(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            variable = service.create("Variable")
            assert variable.id == 1

    def test_variable_get_by_name(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_name("Variable")

    def test_variable_get_by_id(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_variable_list(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_variable_tabulate(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_variable_delete(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestVariableAuthDavePublic(
    auth.DaveTest, auth.PublicPlatformTest, VariableServiceTest
):
    def test_variable_create(
        self, service: VariableService, unauthorized_service: VariableService
    ) -> None:
        with pytest.raises(Forbidden):
            variable = service.create("Variable")
            assert variable.id == 1
        unauthorized_service.create("Variable")

    def test_variable_get_by_name(self, service: VariableService) -> None:
        variable = service.get_by_name("Variable")
        assert variable.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        variable = service.get_by_id(1)
        assert variable.name == "Variable"

    def test_variable_list(self, service: VariableService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestVariableAuthNonePublic(
    auth.NoneTest, auth.PublicPlatformTest, VariableServiceTest
):
    def test_variable_create(
        self, service: VariableService, unauthorized_service: VariableService
    ) -> None:
        with pytest.raises(Forbidden):
            variable = service.create("Variable")
            assert variable.id == 1
        unauthorized_service.create("Variable")

    def test_variable_get_by_name(self, service: VariableService) -> None:
        variable = service.get_by_name("Variable")
        assert variable.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        variable = service.get_by_id(1)
        assert variable.name == "Variable"

    def test_variable_list(self, service: VariableService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
