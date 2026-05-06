import httpx
import pandas as pd
import pytest

from ixmp4.data.dataframe import serialize_df
from ixmp4.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import DirectTransport
from tests.api.base import ApiServiceTest, api_transport, assert_frame_payload

transport = api_transport


class TimeSeriesApiTest(ApiServiceTest[TimeSeriesService]):
    service_class = TimeSeriesService


def create_related(direct_transport: DirectTransport) -> None:
    RegionService(direct_transport).create("Region 1", "default")
    UnitService(direct_transport).create("Unit 1")


class TestTimeSeriesBulkUpsert(TimeSeriesApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        runs = RunService(direct_transport)
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        create_related(direct_transport)
        return run

    def test_timeseries_bulk_upsert_and_tabulate(
        self, client: httpx.Client, run: Run
    ) -> None:
        upsert_df = pd.DataFrame(
            [[run.id, "Region 1", "Variable 1", "Unit 1"]],
            columns=["run__id", "region", "variable", "unit"],
        )
        self.request(
            client,
            "POST",
            "/iamc/timeseries/bulk-upsert",
            json={"df": serialize_df(upsert_df)},
        )

        tabulated = self.request(
            client,
            "PATCH",
            "/iamc/timeseries/tabulate",
            json={"join_parameters": True},
        ).json()

        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "run__id", "region", "variable", "unit"},
        )

    def test_timeseries_tabulate_by_df(self, client: httpx.Client, run: Run) -> None:
        upsert_df = pd.DataFrame(
            [[run.id, "Region 1", "Variable 1", "Unit 1"]],
            columns=["run__id", "region", "variable", "unit"],
        )
        self.request(
            client,
            "POST",
            "/iamc/timeseries/bulk-upsert",
            json={"df": serialize_df(upsert_df)},
        )
        lookup_df = pd.DataFrame(
            [[run.id, "Region 1", "Variable 1", "Unit 1"]],
            columns=["run__id", "region", "variable", "unit"],
        )

        by_df = self.request(
            client,
            "PATCH",
            "/iamc/timeseries/tabulate-by-df",
            json={"df": serialize_df(lookup_df)},
        ).json()

        assert_frame_payload(
            by_df,
            expected_columns={"id", "run__id", "region", "variable", "unit"},
        )
