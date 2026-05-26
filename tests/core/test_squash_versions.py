from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest
import sqlalchemy as sa

import ixmp4
from ixmp4.core.checkpoint import CheckpointView
from ixmp4.core.platform import Platform
from ixmp4.data.checkpoint.db import Checkpoint
from ixmp4.data.versions.model import Operation
from ixmp4.data.versions.squash import squash_version_records
from ixmp4.data.versions.transaction import Transaction
from ixmp4.transport import DirectTransport
from tests import backends
from tests.base import DataFrameTest

from .base import PlatformTest

platform = backends.get_platform_fixture(backends=["postgres"], scope="class")


class SquashTest(PlatformTest, DataFrameTest):
    @pytest.fixture(scope="class")
    def engine(self, platform: Platform) -> sa.Engine:
        transport = platform.backend.transport
        if not isinstance(transport, DirectTransport):
            pytest.skip("squash requires a direct (non-HTTP) transport")
        assert transport.session.bind is not None
        if transport.session.bind.dialect.name != "postgresql":
            pytest.skip("squash requires a PostgreSQL backend")
        return transport.session.bind.engine

    @staticmethod
    def squash(engine: sa.Engine) -> None:
        with engine.connect() as conn:
            squash_version_records(conn)
            conn.commit()

    @staticmethod
    def get_all_tx_ids(engine: sa.Engine) -> set[int]:
        with engine.connect() as conn:
            rows = conn.execute(sa.select(Transaction.id)).fetchall()
        return {r[0] for r in rows}

    @staticmethod
    def get_checkpoint_tx_ids(engine: sa.Engine) -> set[int]:
        with engine.connect() as conn:
            rows = conn.execute(
                sa.select(Checkpoint.transaction__id).where(
                    Checkpoint.transaction__id.isnot(None)
                )
            ).fetchall()
        return {r[0] for r in rows}

    @staticmethod
    def get_all_version_tx_ids(engine: sa.Engine) -> set[int]:
        """Collect every transaction_id and end_transaction_id from all
        version tables."""
        import ixmp4.db.models  # noqa: F401
        from ixmp4.data.versions import PostgresVersionTriggers

        ids: set[int] = set()
        with engine.connect() as conn:
            for trig in PostgresVersionTriggers._registry.values():
                vtbl = trig.version_table
                ids.update(
                    r[0]
                    for r in conn.execute(sa.select(vtbl.c.transaction_id)).fetchall()
                )
                ids.update(
                    r[0]
                    for r in conn.execute(
                        sa.select(vtbl.c.end_transaction_id).where(
                            vtbl.c.end_transaction_id.isnot(None)
                        )
                    ).fetchall()
                )
        return ids

    @staticmethod
    def get_checkpoint_id(run: ixmp4.Run, message: str) -> int:
        """Return the id of the checkpoint with the given message."""
        cp_df = run.checkpoints.tabulate()
        return int(cp_df[cp_df["message"] == message]["id"].iloc[0])

    @staticmethod
    def get_run_checkpoint_tx_id(run: ixmp4.Run, message: str) -> int:
        """Return the transaction id of the checkpoint with the given message."""
        cp_df = run.checkpoints.tabulate()
        return int(cp_df[cp_df["message"] == message]["transaction__id"].iloc[0])

    @classmethod
    def get_run_checkpoint_tx_ids(cls, run: ixmp4.Run, messages: list[str]) -> set[int]:
        """Return transaction ids for the given checkpoint messages."""
        return {cls.get_run_checkpoint_tx_id(run, message) for message in messages}

    @classmethod
    def assert_no_orphaned_transactions(cls, engine: sa.Engine) -> None:
        all_txs = cls.get_all_tx_ids(engine)
        referenced = cls.get_all_version_tx_ids(engine) | cls.get_checkpoint_tx_ids(
            engine
        )
        orphaned = all_txs - referenced
        assert not orphaned, f"Orphaned transactions after squash: {orphaned}"

    def assert_expected_version_rows(
        self, actual: pd.DataFrame, expected: pd.DataFrame
    ) -> None:
        """Compare version rows using expected columns and canonical sorting."""
        pdt.assert_frame_equal(
            self.canonical_sort(actual[list(expected.columns)]),
            self.canonical_sort(expected),
            check_dtype=False,
        )


class TestSquashMetaIndicators(SquashTest):
    """Squash with complex run-meta history across three checkpoints.

    Timeline
    --------
    Checkpoint 1  set three meta keys -> checkpoint-1
    Checkpoint 2  update one key, add one key, delete one key -> checkpoint-2
    Checkpoint 3  add another key (no checkpoint)
    Squash
    """

    @pytest.fixture(scope="class")
    def run(self, platform: Platform) -> ixmp4.Run:
        return platform.runs.create("MetaModel", "MetaScenario")

    @pytest.fixture(scope="class", autouse=True)
    def squash_scenario(self, run: ixmp4.Run, engine: sa.Engine) -> None:
        with run.transact("checkpoint-1"):
            run.meta = {
                "scenario_type": "baseline",
                "year_start": 2020,
                "status": "draft",
            }

        with run.transact("checkpoint-2"):
            run.meta["status"] = "final"
            run.meta["peer_reviewed"] = True
            del run.meta["year_start"]

        with run.transact("checkpoint-3"):
            run.meta["draft_notes"] = "post-checkpoint addition"

        self.squash(engine)

    @pytest.fixture(scope="class")
    def expected_cp1_meta(self) -> dict[str, Any]:
        return {"scenario_type": "baseline", "year_start": 2020, "status": "draft"}

    @pytest.fixture(scope="class")
    def expected_cp2_meta(self) -> dict[str, Any]:
        return {"scenario_type": "baseline", "status": "final", "peer_reviewed": True}

    def test_meta_values_correct_after_squash(self, run: ixmp4.Run) -> None:
        """All meta mutations are visible through the facade after squash."""
        expected = {
            "scenario_type": "baseline",
            "status": "final",
            "peer_reviewed": True,
            "draft_notes": "post-checkpoint addition",
        }
        assert dict(run.meta) == expected

    def test_checkpoint_meta_views_correct_after_squash(
        self,
        run: ixmp4.Run,
        expected_cp1_meta: dict[str, Any],
        expected_cp2_meta: dict[str, Any],
    ) -> None:
        """Checkpoint meta views show the expected state after squashing."""
        cp1_id = self.get_checkpoint_id(run, "checkpoint-1")
        cp2_id = self.get_checkpoint_id(run, "checkpoint-2")
        assert dict(run.checkpoints[cp1_id].meta) == expected_cp1_meta
        assert dict(run.checkpoints[cp2_id].meta) == expected_cp2_meta

    @pytest.fixture(scope="class")
    def expected_meta_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        INS = int(Operation.INSERT)
        DEL = int(Operation.DELETE)
        return pd.DataFrame(
            [
                ["draft_notes", cp3, None, INS],
                ["peer_reviewed", cp2, None, INS],
                ["scenario_type", cp1, None, INS],
                ["status", cp1, cp2, INS],  #       (draft, superseded in Checkpoint 2)
                ["status", cp2, None, DEL],
                ["status", cp2, None, INS],  #      (final)
                ["year_start", cp1, cp2, INS],  #   (deleted in Checkpoint 2)
                ["year_start", cp2, None, DEL],
            ],
            columns=["key", "transaction_id", "end_transaction_id", "operation_type"],
        )

    def test_meta_version_table_state(
        self,
        run: ixmp4.Run,
        platform: Platform,
        expected_meta_version_table: pd.DataFrame,
    ) -> None:
        """After squash, version rows match the expected checkpoint-aligned table."""
        vdf = platform.backend.meta.tabulate_versions(run__id=run.id)
        self.assert_expected_version_rows(vdf, expected_meta_version_table)

    def test_no_orphaned_transactions(self, engine: sa.Engine) -> None:
        self.assert_no_orphaned_transactions(engine)


class TestSquashIamcData(SquashTest):
    """Squash with IAMC timeseries data mutated across two checkpoints.

    Timeline
    --------
    Checkpoint 1  add 4 rows (2 regions * 1 variable * 2 years) -> checkpoint-1
    Checkpoint 2  upsert the Region-1 rows with new values -> checkpoint-2
    Checkpoint 3  add 2 rows for a second variable (checkpoint-3)
    Squash
    """

    @pytest.fixture(scope="class")
    def unit(self, platform: Platform) -> ixmp4.Unit:
        return platform.units.create("GtCO2")

    @pytest.fixture(scope="class")
    def regions(self, platform: Platform) -> list[ixmp4.Region]:
        return [
            platform.regions.create("Region 1", "default"),
            platform.regions.create("Region 2", "default"),
        ]

    @pytest.fixture(scope="class")
    def run(self, platform: Platform) -> ixmp4.Run:
        return platform.runs.create("IamcModel", "IamcScenario")

    @pytest.fixture(scope="class", autouse=True)
    def squash_scenario(
        self,
        run: ixmp4.Run,
        engine: sa.Engine,
        unit: ixmp4.Unit,
        regions: list[ixmp4.Region],
    ) -> None:
        phase1 = pd.DataFrame(
            [
                ["Region 1", "GtCO2", "Emissions|CO2", 2020, 1.1],
                ["Region 1", "GtCO2", "Emissions|CO2", 2030, 1.3],
                ["Region 2", "GtCO2", "Emissions|CO2", 2020, 2.1],
                ["Region 2", "GtCO2", "Emissions|CO2", 2030, 2.3],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        phase1["year"] = phase1["year"].astype("Int64")

        with run.transact("checkpoint-1"):
            run.iamc.add(phase1)

        phase2_upsert = pd.DataFrame(
            [
                ["Region 1", "GtCO2", "Emissions|CO2", 2020, 99.0],
                ["Region 1", "GtCO2", "Emissions|CO2", 2030, 99.9],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        phase2_upsert["year"] = phase2_upsert["year"].astype("Int64")

        with run.transact("checkpoint-2"):
            run.iamc.add(phase2_upsert)

        phase3 = pd.DataFrame(
            [
                ["Region 1", "GtCO2", "GDP|PPP", 2020, 3.1],
                ["Region 1", "GtCO2", "GDP|PPP", 2030, 3.3],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        phase3["year"] = phase3["year"].astype("Int64")

        with run.transact("checkpoint-3"):
            run.iamc.add(phase3)

        self.squash(engine)

    @pytest.fixture(scope="class")
    def expected_cp1_iamc(self) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["Region 1", "GtCO2", "Emissions|CO2", 2020, 1.1],
                ["Region 1", "GtCO2", "Emissions|CO2", 2030, 1.3],
                ["Region 2", "GtCO2", "Emissions|CO2", 2020, 2.1],
                ["Region 2", "GtCO2", "Emissions|CO2", 2030, 2.3],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def expected_cp2_iamc(self) -> pd.DataFrame:
        # Region 1 has been updated; Region 2 unchanged; Checkpoint 3 GDP not yet added
        df = pd.DataFrame(
            [
                ["Region 1", "GtCO2", "Emissions|CO2", 2020, 99.0],
                ["Region 1", "GtCO2", "Emissions|CO2", 2030, 99.9],
                ["Region 2", "GtCO2", "Emissions|CO2", 2020, 2.1],
                ["Region 2", "GtCO2", "Emissions|CO2", 2030, 2.3],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    def test_iamc_data_correct_after_squash(self, run: ixmp4.Run) -> None:
        """All IAMC data mutations are visible through the facade after squash."""
        expected = pd.DataFrame(
            [
                ["Region 1", "GtCO2", "Emissions|CO2", 2020, 99.0],
                ["Region 1", "GtCO2", "Emissions|CO2", 2030, 99.9],
                ["Region 2", "GtCO2", "Emissions|CO2", 2020, 2.1],
                ["Region 2", "GtCO2", "Emissions|CO2", 2030, 2.3],
                ["Region 1", "GtCO2", "GDP|PPP", 2020, 3.1],
                ["Region 1", "GtCO2", "GDP|PPP", 2030, 3.3],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        expected["year"] = expected["year"].astype("Int64")

        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            ret.sort_values(["variable", "region", "year"]).reset_index(drop=True),
            expected.sort_values(["variable", "region", "year"]).reset_index(drop=True),
            check_like=True,
        )

    def test_checkpoint_iamc_views_correct_after_squash(
        self,
        run: ixmp4.Run,
        expected_cp1_iamc: pd.DataFrame,
        expected_cp2_iamc: pd.DataFrame,
    ) -> None:
        """Checkpoint IAMC views show the expected state after squashing."""
        cp1_id = self.get_checkpoint_id(run, "checkpoint-1")
        cp2_id = self.get_checkpoint_id(run, "checkpoint-2")

        def _sort(df: pd.DataFrame) -> pd.DataFrame:
            return df.sort_values(["variable", "region", "year"]).reset_index(drop=True)

        pdt.assert_frame_equal(
            _sort(run.checkpoints[cp1_id].iamc.tabulate()),
            _sort(expected_cp1_iamc),
            check_like=True,
        )
        pdt.assert_frame_equal(
            _sort(run.checkpoints[cp2_id].iamc.tabulate()),
            _sort(expected_cp2_iamc),
            check_like=True,
        )

    @pytest.fixture(scope="class")
    def expected_datapoint_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        INS = int(Operation.INSERT)
        UPD = int(Operation.UPDATE)
        return pd.DataFrame(
            [
                [1.1, cp1, cp2, INS],  #   (Region1 CO2 2020 original, superseded)
                [1.3, cp1, cp2, INS],  #   (Region1 CO2 2030 original, superseded)
                [2.1, cp1, None, INS],  #  (Region2 CO2 2020, unchanged)
                [2.3, cp1, None, INS],  #  (Region2 CO2 2030, unchanged)
                [3.1, cp3, None, INS],  #  (GDP|PPP 2020, checkpoint-3)
                [3.3, cp3, None, INS],  #  (GDP|PPP 2030, checkpoint-3)
                [99.0, cp2, None, UPD],  # (Region1 CO2 2020 updated)
                [99.9, cp2, None, UPD],  # (Region1 CO2 2030 updated)
            ],
            columns=["value", "transaction_id", "end_transaction_id", "operation_type"],
        )

    def test_datapoint_version_table_state(
        self,
        run: ixmp4.Run,
        platform: Platform,
        expected_datapoint_version_table: pd.DataFrame,
    ) -> None:
        """After squash, datapoint version rows match the expected checkpoint-aligned
        table."""
        vdf = platform.backend.iamc.datapoints.tabulate_versions(run={"id": run.id})
        self.assert_expected_version_rows(vdf, expected_datapoint_version_table)

    def test_no_orphaned_transactions(self, engine: sa.Engine) -> None:
        self.assert_no_orphaned_transactions(engine)


class TestSquashOptimizationData(SquashTest):
    """Squash with optimization objects mutated across two checkpoints.

    Timeline
    --------
    Checkpoint 1  IndexSet "Regions" + data, Scalar "discount_rate"=0.05 -> checkpoint-1
    Checkpoint 2  update scalar to 0.03, extend IndexSet data -> checkpoint-2
    Checkpoint 3  create IndexSet "Years" + data (no checkpoint)
    Squash
    """

    @pytest.fixture(scope="class")
    def unit(self, platform: Platform) -> ixmp4.Unit:
        return platform.units.create("percent")

    @pytest.fixture(scope="class")
    def run(self, platform: Platform) -> ixmp4.Run:
        return platform.runs.create("OptModel", "OptScenario")

    @pytest.fixture(scope="class", autouse=True)
    def squash_scenario(
        self, run: ixmp4.Run, engine: sa.Engine, unit: ixmp4.Unit
    ) -> None:
        with run.transact("checkpoint-1"):
            regions = run.optimization.indexsets.create("Regions")
            regions.add_data(["DE", "FR", "US"])
            run.optimization.scalars.create("discount_rate", 0.05, unit.name)

        with run.transact("checkpoint-2"):
            scalar = run.optimization.scalars.get_by_name("discount_rate")
            scalar.update(value=0.03)
            regions = run.optimization.indexsets.get_by_name("Regions")
            regions.add_data("GB")

        with run.transact("checkpoint-3"):
            years = run.optimization.indexsets.create("Years")
            years.add_data([2025, 2030, 2035])

        self.squash(engine)

    @pytest.fixture(scope="class")
    def expected_cp1_scalar_values(self) -> dict[str, float]:
        return {"discount_rate": 0.05}

    @pytest.fixture(scope="class")
    def expected_cp2_scalar_values(self) -> dict[str, float]:
        return {"discount_rate": 0.03}

    @pytest.fixture(scope="class")
    def expected_cp1_indexset_names(self) -> set[str]:
        return {"Regions"}

    @pytest.fixture(scope="class")
    def expected_cp2_indexset_names(self) -> set[str]:
        # Years is created in checkpoint-3, after checkpoint-2
        return {"Regions"}

    def test_scalar_value_correct_after_squash(self, run: ixmp4.Run) -> None:
        scalar = run.optimization.scalars.get_by_name("discount_rate")
        assert scalar.value == 0.03

    def test_indexset_data_extended_correctly(self, run: ixmp4.Run) -> None:
        regions = run.optimization.indexsets.get_by_name("Regions")
        assert sorted(regions.data) == ["DE", "FR", "GB", "US"]

    def test_post_checkpoint_indexset_intact_after_squash(self, run: ixmp4.Run) -> None:
        years = run.optimization.indexsets.get_by_name("Years")
        assert sorted(years.data) == [2025, 2030, 2035]

    def test_checkpoint_optimization_views_correct_after_squash(
        self,
        run: ixmp4.Run,
        expected_cp1_scalar_values: dict[str, float],
        expected_cp2_scalar_values: dict[str, float],
        expected_cp1_indexset_names: set[str],
        expected_cp2_indexset_names: set[str],
    ) -> None:
        """Checkpoint optimization views show the expected state after squashing."""
        cp1_id = self.get_checkpoint_id(run, "checkpoint-1")
        cp2_id = self.get_checkpoint_id(run, "checkpoint-2")

        def scalar_values(cp_view: CheckpointView) -> dict[str, float]:
            df = cp_view.optimization.scalars.tabulate()
            return dict(zip(df["name"], df["value"].map(float)))

        def indexset_names(cp_view: CheckpointView) -> set[str]:
            return set(cp_view.optimization.indexsets.tabulate()["name"])

        assert scalar_values(run.checkpoints[cp1_id]) == pytest.approx(
            expected_cp1_scalar_values
        )
        assert scalar_values(run.checkpoints[cp2_id]) == pytest.approx(
            expected_cp2_scalar_values
        )
        assert indexset_names(run.checkpoints[cp1_id]) == expected_cp1_indexset_names
        assert indexset_names(run.checkpoints[cp2_id]) == expected_cp2_indexset_names

    @pytest.fixture(scope="class")
    def expected_scalar_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        ins = int(Operation.INSERT)
        upd = int(Operation.UPDATE)
        return pd.DataFrame(
            [
                ["discount_rate", cp1, cp2, ins, 0.05],
                ["discount_rate", cp2, None, upd, 0.03],
            ],
            columns=[
                "name",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
                "value",
            ],
        )

    def test_scalar_version_table_state(
        self,
        run: ixmp4.Run,
        platform: Platform,
        expected_scalar_version_table: pd.DataFrame,
    ) -> None:
        """After squash, scalar version rows match expected checkpoint-aligned rows."""
        vdf = platform.backend.optimization.scalars.tabulate_versions(run__id=run.id)
        self.assert_expected_version_rows(vdf, expected_scalar_version_table)

    @pytest.fixture(scope="class")
    def expected_indexset_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        ins = int(Operation.INSERT)
        upd = int(Operation.UPDATE)
        return pd.DataFrame(
            [
                ["Regions", cp1, cp2, ins],
                ["Regions", cp2, None, upd],
                ["Years", cp3, None, ins],
            ],
            columns=["name", "transaction_id", "end_transaction_id", "operation_type"],
        )

    @pytest.fixture(scope="class")
    def expected_indexset_data_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        ins = int(Operation.INSERT)
        return pd.DataFrame(
            [
                ["DE", cp1, None, ins],
                ["FR", cp1, None, ins],
                ["GB", cp2, None, ins],
                ["US", cp1, None, ins],
                ["2025", cp3, None, ins],
                ["2030", cp3, None, ins],
                ["2035", cp3, None, ins],
            ],
            columns=[
                "value",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

    def test_indexset_version_table_state(
        self,
        run: ixmp4.Run,
        platform: Platform,
        expected_indexset_version_table: pd.DataFrame,
        expected_indexset_data_version_table: pd.DataFrame,
    ) -> None:
        """After squash, IndexSet and IndexSetData rows match expected tables."""
        is_df = platform.backend.optimization.indexsets.tabulate_versions(
            run__id=run.id
        )

        self.assert_expected_version_rows(is_df, expected_indexset_version_table)

        isd_vdf = platform.backend.optimization.indexsets.tabulate_data_versions(
            indexset__id__in=is_df["id"].tolist()
        )
        self.assert_expected_version_rows(
            isd_vdf,
            expected_indexset_data_version_table,
        )

    def test_no_orphaned_transactions(self, engine: sa.Engine) -> None:
        self.assert_no_orphaned_transactions(engine)


class TestSquashRunLockFreeRegionEdges(SquashTest):
    """Squash behavior for datatypes not affected by versioning/rollback
    (regions, units) around checkpoints."""

    @pytest.fixture(scope="class")
    def unit(self, platform: Platform) -> ixmp4.Unit:
        return platform.units.create("unit")

    @pytest.fixture(scope="class")
    def run(self, platform: Platform) -> ixmp4.Run:
        return platform.runs.create("RegionEdgeModel", "RegionEdgeScenario")

    @pytest.fixture(scope="class", autouse=True)
    def squash_scenario(
        self,
        platform: Platform,
        run: ixmp4.Run,
        engine: sa.Engine,
        unit: ixmp4.Unit,
    ) -> None:

        with run.transact("checkpoint-1"):
            run.meta = {"status": "draft"}
            run.optimization.scalars.create("edge_scalar", 1.0, unit.name)

        # Region created and deleted between checkpoint-1 and checkpoint-2.
        r_between = platform.regions.create("Region Between", "edge")
        r_between.delete()

        with run.transact("checkpoint-2"):
            run.meta["status"] = "final"
            scalar = run.optimization.scalars.get_by_name("edge_scalar")
            scalar.update(value=2.0)

        # Region created before checkpoint-3, then deleted after checkpoint-3.
        platform.regions.create("Region Deleted After Last", "edge")

        with run.transact("checkpoint-3"):
            run.meta["note"] = "checkpoint-3"

        # Region created after last checkpoint.
        platform.regions.create("Region Created After Last", "edge")
        platform.regions.delete("Region Deleted After Last")

        self.squash(engine)

    @pytest.fixture(scope="class")
    def expected_region_version_table(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                ["Region Between", int(Operation.DELETE)],
                ["Region Created After Last", int(Operation.INSERT)],
                ["Region Deleted After Last", int(Operation.INSERT)],
                ["Region Deleted After Last", int(Operation.DELETE)],
            ],
            columns=["name", "operation_type"],
        )

    @pytest.fixture(scope="class")
    def expected_edge_meta_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        return pd.DataFrame(
            [
                ["note", cp3, None, int(Operation.INSERT)],
                ["status", cp1, cp2, int(Operation.INSERT)],
                ["status", cp2, None, int(Operation.DELETE)],
                ["status", cp2, None, int(Operation.INSERT)],
            ],
            columns=["key", "transaction_id", "end_transaction_id", "operation_type"],
        )

    @pytest.fixture(scope="class")
    def expected_edge_scalar_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        return pd.DataFrame(
            [
                ["edge_scalar", cp1, cp2, int(Operation.INSERT), 1.0],
                ["edge_scalar", cp2, None, int(Operation.UPDATE), 2.0],
            ],
            columns=[
                "name",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
                "value",
            ],
        )

    def test_region_edges_and_other_data_intact_after_squash(
        self,
        run: ixmp4.Run,
        platform: Platform,
        expected_region_version_table: pd.DataFrame,
        expected_edge_meta_version_table: pd.DataFrame,
        expected_edge_scalar_version_table: pd.DataFrame,
    ) -> None:
        """Region/meta/scalar version rows match expected post-squash tables."""
        rvdf = platform.backend.regions.tabulate_versions()

        self.assert_expected_version_rows(
            rvdf.assign(operation_type=rvdf["operation_type"].astype(int)),
            expected_region_version_table,
        )

        meta_vdf = platform.backend.meta.tabulate_versions(run__id=run.id)
        self.assert_expected_version_rows(meta_vdf, expected_edge_meta_version_table)

        scalar_vdf = platform.backend.optimization.scalars.tabulate_versions(
            run__id=run.id
        )
        self.assert_expected_version_rows(
            scalar_vdf,
            expected_edge_scalar_version_table,
        )

        assert dict(run.meta) == {"status": "final", "note": "checkpoint-3"}
        scalar = run.optimization.scalars.get_by_name("edge_scalar")
        assert scalar.value == pytest.approx(2.0)

        cp1_id = self.get_checkpoint_id(run, "checkpoint-1")
        cp2_id = self.get_checkpoint_id(run, "checkpoint-2")
        cp3_id = self.get_checkpoint_id(run, "checkpoint-3")
        assert dict(run.checkpoints[cp1_id].meta) == {"status": "draft"}
        assert dict(run.checkpoints[cp2_id].meta) == {"status": "final"}
        assert dict(run.checkpoints[cp3_id].meta) == {
            "status": "final",
            "note": "checkpoint-3",
        }

    def test_no_orphaned_transactions(self, engine: sa.Engine) -> None:
        self.assert_no_orphaned_transactions(engine)


class TestSquashCombinedScenario(SquashTest):
    """Squash with all three data types interleaved across two checkpoints.

    A single run combines meta indicators, IAMC timeseries data, and
    optimization objects.  Each phase touches all three data types, creating
    interleaved version records across the full breadth of version tables.

    Timeline
    --------
    Checkpoint 1  meta + IAMC (4 rows) + opt (IndexSet + Scalar) -> checkpoint-1
    Checkpoint 2  update meta + upsert IAMC (2 rows) + update Scalar + extend
             IndexSet -> checkpoint-2
    Checkpoint 3  add meta key + add IAMC variable + add opt IndexSet (no checkpoint)
    Squash
    """

    @pytest.fixture(scope="class")
    def unit(self, platform: Platform) -> ixmp4.Unit:
        return platform.units.create("EJ")

    @pytest.fixture(scope="class")
    def regions(self, platform: Platform) -> list[ixmp4.Region]:
        return [
            platform.regions.create("EU", "default"),
            platform.regions.create("CN", "default"),
        ]

    @pytest.fixture(scope="class")
    def run(self, platform: Platform) -> ixmp4.Run:
        return platform.runs.create("CombinedModel", "CombinedScenario")

    @pytest.fixture(scope="class", autouse=True)
    def squash_scenario(
        self,
        run: ixmp4.Run,
        engine: sa.Engine,
        unit: ixmp4.Unit,
        regions: list[ixmp4.Region],
    ) -> None:
        phase1_iamc = pd.DataFrame(
            [
                ["EU", "EJ", "Primary Energy", 2020, 50.0],
                ["EU", "EJ", "Primary Energy", 2030, 55.0],
                ["CN", "EJ", "Primary Energy", 2020, 80.0],
                ["CN", "EJ", "Primary Energy", 2030, 90.0],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        phase1_iamc["year"] = phase1_iamc["year"].astype("Int64")

        with run.transact("checkpoint-1"):
            run.meta = {"model_version": "1.0", "status": "draft", "run_count": 1}
            run.iamc.add(phase1_iamc)
            sectors = run.optimization.indexsets.create("Sectors")
            sectors.add_data(["Energy", "Industry", "Transport"])
            run.optimization.scalars.create("carbon_price", 25.0, unit.name)

        phase2_upsert = pd.DataFrame(
            [
                ["EU", "EJ", "Primary Energy", 2020, 52.5],
                ["EU", "EJ", "Primary Energy", 2030, 57.5],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        phase2_upsert["year"] = phase2_upsert["year"].astype("Int64")

        with run.transact("checkpoint-2"):
            run.meta["status"] = "validated"
            run.meta["run_count"] = 2
            del run.meta["model_version"]
            run.iamc.add(phase2_upsert)
            sectors = run.optimization.indexsets.get_by_name("Sectors")
            sectors.add_data("Agriculture")
            scalar = run.optimization.scalars.get_by_name("carbon_price")
            scalar.update(value=50.0)

        phase3_iamc = pd.DataFrame(
            [
                ["EU", "EJ", "Final Energy", 2020, 40.0],
                ["CN", "EJ", "Final Energy", 2020, 65.0],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        phase3_iamc["year"] = phase3_iamc["year"].astype("Int64")

        with run.transact("checkpoint-3"):
            run.meta["next_run_note"] = "scheduled for Q3"
            run.iamc.add(phase3_iamc)
            technologies = run.optimization.indexsets.create("Technologies")
            technologies.add_data(["Solar", "Wind", "Nuclear"])

        self.squash(engine)

    @pytest.fixture(scope="class")
    def expected_cp1_meta(self) -> dict[str, Any]:
        return {"model_version": "1.0", "status": "draft", "run_count": 1}

    @pytest.fixture(scope="class")
    def expected_cp2_meta(self) -> dict[str, Any]:
        # model_version deleted in Checkpoint 2; run_count and status updated
        return {"status": "validated", "run_count": 2}

    @pytest.fixture(scope="class")
    def expected_cp1_iamc(self) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["EU", "EJ", "Primary Energy", 2020, 50.0],
                ["EU", "EJ", "Primary Energy", 2030, 55.0],
                ["CN", "EJ", "Primary Energy", 2020, 80.0],
                ["CN", "EJ", "Primary Energy", 2030, 90.0],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def expected_cp2_iamc(self) -> pd.DataFrame:
        # EU updated; CN unchanged; Final Energy not yet added
        df = pd.DataFrame(
            [
                ["EU", "EJ", "Primary Energy", 2020, 52.5],
                ["EU", "EJ", "Primary Energy", 2030, 57.5],
                ["CN", "EJ", "Primary Energy", 2020, 80.0],
                ["CN", "EJ", "Primary Energy", 2030, 90.0],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def expected_cp1_scalar_values(self) -> dict[str, float]:
        return {"carbon_price": 25.0}

    @pytest.fixture(scope="class")
    def expected_cp2_scalar_values(self) -> dict[str, float]:
        return {"carbon_price": 50.0}

    @pytest.fixture(scope="class")
    def expected_cp1_indexset_names(self) -> set[str]:
        return {"Sectors"}

    @pytest.fixture(scope="class")
    def expected_cp2_indexset_names(self) -> set[str]:
        # Technologies added in Checkpoint 3 (after checkpoint-2)
        return {"Sectors"}

    def test_meta_correct_after_squash(self, run: ixmp4.Run) -> None:
        expected = {
            "status": "validated",
            "run_count": 2,
            "next_run_note": "scheduled for Q3",
        }
        assert dict(run.meta) == expected

    def test_iamc_data_correct_after_squash(self, run: ixmp4.Run) -> None:
        expected = pd.DataFrame(
            [
                ["EU", "EJ", "Primary Energy", 2020, 52.5],
                ["EU", "EJ", "Primary Energy", 2030, 57.5],
                ["CN", "EJ", "Primary Energy", 2020, 80.0],
                ["CN", "EJ", "Primary Energy", 2030, 90.0],
                ["EU", "EJ", "Final Energy", 2020, 40.0],
                ["CN", "EJ", "Final Energy", 2020, 65.0],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        expected["year"] = expected["year"].astype("Int64")

        ret = run.iamc.tabulate()
        pdt.assert_frame_equal(
            ret.sort_values(["variable", "region", "year"]).reset_index(drop=True),
            expected.sort_values(["variable", "region", "year"]).reset_index(drop=True),
            check_like=True,
        )

    def test_scalar_correct_after_squash(self, run: ixmp4.Run) -> None:
        scalar = run.optimization.scalars.get_by_name("carbon_price")
        assert scalar.value == 50.0

    def test_sectors_indexset_correct_after_squash(self, run: ixmp4.Run) -> None:
        sectors = run.optimization.indexsets.get_by_name("Sectors")
        assert sorted(sectors.data) == [
            "Agriculture",
            "Energy",
            "Industry",
            "Transport",
        ]

    def test_post_checkpoint_indexset_correct_after_squash(
        self, run: ixmp4.Run
    ) -> None:
        techs = run.optimization.indexsets.get_by_name("Technologies")
        assert sorted(techs.data) == ["Nuclear", "Solar", "Wind"]

    def test_checkpoint_views_correct_after_squash(
        self,
        run: ixmp4.Run,
        expected_cp1_meta: dict[str, Any],
        expected_cp2_meta: dict[str, Any],
        expected_cp1_iamc: pd.DataFrame,
        expected_cp2_iamc: pd.DataFrame,
        expected_cp1_scalar_values: dict[str, float],
        expected_cp2_scalar_values: dict[str, float],
        expected_cp1_indexset_names: set[str],
        expected_cp2_indexset_names: set[str],
    ) -> None:
        """All checkpoint views show the expected state after squashing."""
        cp1_id = self.get_checkpoint_id(run, "checkpoint-1")
        cp2_id = self.get_checkpoint_id(run, "checkpoint-2")

        # meta
        assert dict(run.checkpoints[cp1_id].meta) == expected_cp1_meta
        assert dict(run.checkpoints[cp2_id].meta) == expected_cp2_meta

        # IAMC
        def _sort_iamc(df: pd.DataFrame) -> pd.DataFrame:
            return df.sort_values(["variable", "region", "year"]).reset_index(drop=True)

        pdt.assert_frame_equal(
            _sort_iamc(run.checkpoints[cp1_id].iamc.tabulate()),
            _sort_iamc(expected_cp1_iamc),
            check_like=True,
        )
        pdt.assert_frame_equal(
            _sort_iamc(run.checkpoints[cp2_id].iamc.tabulate()),
            _sort_iamc(expected_cp2_iamc),
            check_like=True,
        )

        # optimization
        def scalar_values(cp_view: CheckpointView) -> dict[str, float]:
            df = cp_view.optimization.scalars.tabulate()
            return dict(zip(df["name"], df["value"].map(float)))

        def indexset_names(cp_view: CheckpointView) -> set[str]:
            return set(cp_view.optimization.indexsets.tabulate()["name"])

        assert scalar_values(run.checkpoints[cp1_id]) == pytest.approx(
            expected_cp1_scalar_values
        )
        assert scalar_values(run.checkpoints[cp2_id]) == pytest.approx(
            expected_cp2_scalar_values
        )
        assert indexset_names(run.checkpoints[cp1_id]) == expected_cp1_indexset_names
        assert indexset_names(run.checkpoints[cp2_id]) == expected_cp2_indexset_names

    @pytest.fixture(scope="class")
    def expected_combined_meta_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        return pd.DataFrame(
            [
                ["model_version", cp1, cp2, int(Operation.INSERT)],
                ["model_version", cp2, None, int(Operation.DELETE)],
                ["next_run_note", cp3, None, int(Operation.INSERT)],
                ["run_count", cp1, cp2, int(Operation.INSERT)],
                ["run_count", cp2, None, int(Operation.DELETE)],
                ["run_count", cp2, None, int(Operation.INSERT)],
                ["status", cp1, cp2, int(Operation.INSERT)],
                ["status", cp2, None, int(Operation.DELETE)],
                ["status", cp2, None, int(Operation.INSERT)],
            ],
            columns=["key", "transaction_id", "end_transaction_id", "operation_type"],
        )

    @pytest.fixture(scope="class")
    def expected_combined_datapoint_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        return pd.DataFrame(
            [
                [40.0, cp3, None, int(Operation.INSERT)],
                [50.0, cp1, cp2, int(Operation.INSERT)],
                [52.5, cp2, None, int(Operation.UPDATE)],
                [55.0, cp1, cp2, int(Operation.INSERT)],
                [57.5, cp2, None, int(Operation.UPDATE)],
                [65.0, cp3, None, int(Operation.INSERT)],
                [80.0, cp1, None, int(Operation.INSERT)],
                [90.0, cp1, None, int(Operation.INSERT)],
            ],
            columns=["value", "transaction_id", "end_transaction_id", "operation_type"],
        )

    @pytest.fixture(scope="class")
    def expected_combined_scalar_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        return pd.DataFrame(
            [
                ["carbon_price", cp1, cp2, int(Operation.INSERT), 25.0],
                ["carbon_price", cp2, None, int(Operation.UPDATE), 50.0],
            ],
            columns=[
                "name",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
                "value",
            ],
        )

    @pytest.fixture(scope="class")
    def expected_combined_indexset_version_table(self, run: ixmp4.Run) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        return pd.DataFrame(
            [
                ["Sectors", cp1, cp2, int(Operation.INSERT)],
                ["Sectors", cp2, None, int(Operation.UPDATE)],
                ["Technologies", cp3, None, int(Operation.INSERT)],
            ],
            columns=["name", "transaction_id", "end_transaction_id", "operation_type"],
        )

    @pytest.fixture(scope="class")
    def expected_combined_indexset_data_version_table(
        self, run: ixmp4.Run
    ) -> pd.DataFrame:
        cp1 = self.get_run_checkpoint_tx_id(run, "checkpoint-1")
        cp2 = self.get_run_checkpoint_tx_id(run, "checkpoint-2")
        cp3 = self.get_run_checkpoint_tx_id(run, "checkpoint-3")
        return pd.DataFrame(
            [
                ["Agriculture", cp2, None, int(Operation.INSERT)],
                ["Energy", cp1, None, int(Operation.INSERT)],
                ["Industry", cp1, None, int(Operation.INSERT)],
                ["Transport", cp1, None, int(Operation.INSERT)],
                ["Nuclear", cp3, None, int(Operation.INSERT)],
                ["Solar", cp3, None, int(Operation.INSERT)],
                ["Wind", cp3, None, int(Operation.INSERT)],
            ],
            columns=[
                "value",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

    def test_all_version_tables_only_reference_checkpoint_txs(
        self,
        run: ixmp4.Run,
        platform: Platform,
        expected_combined_meta_version_table: pd.DataFrame,
        expected_combined_datapoint_version_table: pd.DataFrame,
        expected_combined_scalar_version_table: pd.DataFrame,
        expected_combined_indexset_version_table: pd.DataFrame,
        expected_combined_indexset_data_version_table: pd.DataFrame,
    ) -> None:
        """After squash, all combined-scenario version tables match expectations."""
        meta_vdf = platform.backend.meta.tabulate_versions(run__id=run.id)
        dp_vdf = platform.backend.iamc.datapoints.tabulate_versions(run={"id": run.id})
        sca_vdf = platform.backend.optimization.scalars.tabulate_versions(
            run__id=run.id
        )
        idx_vdf = platform.backend.optimization.indexsets.tabulate_versions(
            run__id=run.id
        )
        isd_vdf = platform.backend.optimization.indexsets.tabulate_data_versions(
            indexset__id__in=idx_vdf["id"].tolist()
        )

        self.assert_expected_version_rows(
            meta_vdf,
            expected_combined_meta_version_table,
        )
        self.assert_expected_version_rows(
            dp_vdf,
            expected_combined_datapoint_version_table,
        )
        self.assert_expected_version_rows(
            sca_vdf,
            expected_combined_scalar_version_table,
        )
        self.assert_expected_version_rows(
            idx_vdf,
            expected_combined_indexset_version_table,
        )

        self.assert_expected_version_rows(
            isd_vdf,
            expected_combined_indexset_data_version_table,
        )

    def test_no_orphaned_transactions(self, engine: sa.Engine) -> None:
        self.assert_no_orphaned_transactions(engine)
