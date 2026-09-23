import datetime
import logging
from collections.abc import Generator
from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest
import sqlalchemy as sa

import ixmp4
from ixmp4.base_exceptions import Forbidden
from ixmp4.core.run import RunCloner
from ixmp4.data.backend import Backend
from ixmp4.data.region.exceptions import RegionNotFound
from ixmp4.data.run.db import Run
from ixmp4.data.unit.exceptions import UnitNotFound
from ixmp4.transport import DirectTransport
from tests import backends
from tests.core.base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class TestRun(PlatformTest):
    @pytest.fixture(scope="class")
    def units(self, platform: ixmp4.Platform) -> list[ixmp4.Unit]:
        return [platform.units.create("Unit 1")]

    @pytest.fixture(scope="class")
    def regions(self, platform: ixmp4.Platform) -> list[ixmp4.Region]:
        return [platform.regions.create("Region 1", "default")]

    def _add_simple_data(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        df = pd.DataFrame(
            [["Region 1", "Unit 1", "Variable 1", 2000, 1.1]],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        with run.transact("Add simple data"):
            run.meta = {"key": "value"}
            run.iamc.add(df)

    def test_create_run(
        self,
        platform: ixmp4.Platform,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
        fake_time: datetime.datetime,
    ) -> None:
        run1 = platform.runs.create("Model", "Scenario")
        run1.set_as_default()

        run2 = platform.runs.create("Model", "Scenario")
        run3 = platform.runs.create("Other Model", "Scenario")
        run4 = platform.runs.create("Other Model", "Other Scenario")

        assert run1.id == 1
        assert run1.model.name == "Model"
        assert run1.scenario.name == "Scenario"
        assert run1.version == 1

        assert run1.created_at == fake_time.replace(tzinfo=None)
        assert run1.created_by == "@unknown"

        assert str(run1) == "<Run model='Model' scenario='Scenario' version=1 id=1>"

        assert run2.id == 2
        assert run2.version == 2

        assert run3.id == 3
        assert run4.id == 4

        for run in (run1, run2, run3, run4):
            self._add_simple_data(platform, run)

    def test_tabulate_run(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.runs.tabulate(default_only=False)
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "model" in ret_df.columns
        assert "scenario" in ret_df.columns
        assert "version" in ret_df.columns

    def test_tabulate_run_facade_model_scenario_filters(
        self, platform: ixmp4.Platform
    ) -> None:
        shorthand = platform.runs.tabulate(
            model="Model", scenario="Scenario", default_only=False
        )
        explicit = platform.runs.tabulate(
            model={"name__like": "Model"},
            scenario={"name__like": "Scenario"},
            default_only=False,
        )
        pdt.assert_frame_equal(
            shorthand.sort_values(["id"]).reset_index(drop=True),
            explicit.sort_values(["id"]).reset_index(drop=True),
            check_like=True,
        )

    def test_tabulate_run_hides_internal_columns_by_default(
        self, platform: ixmp4.Platform
    ) -> None:
        ret_df = platform.runs.tabulate(default_only=False)
        assert "model__id" not in ret_df.columns
        assert "scenario__id" not in ret_df.columns
        assert "lock_transaction" not in ret_df.columns

    def test_tabulate_run_shows_internal_columns_when_requested(
        self, platform: ixmp4.Platform
    ) -> None:
        ret_df = platform.runs.tabulate(
            default_only=False, include_internal_columns=True
        )
        assert "model__id" in ret_df.columns
        assert "scenario__id" in ret_df.columns
        assert "lock_transaction" in ret_df.columns

    def test_list_run(self, platform: ixmp4.Platform) -> None:
        assert len(platform.runs.list(default_only=False)) == 4

    def test_delete_run_via_func_obj(self, platform: ixmp4.Platform) -> None:
        run1 = platform.runs.get("Model", "Scenario")
        platform.runs.delete(run1)
        run2 = platform.runs.get("Model", "Scenario", version=2)
        platform.runs.delete(run2)

    def test_delete_run_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.runs.delete(3)

    def test_delete_run_via_obj(self, platform: ixmp4.Platform) -> None:
        run4 = platform.runs.get("Other Model", "Other Scenario", version=1)
        run4.delete()

    def test_runs_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.runs.tabulate().empty
        assert len(platform.runs.list()) == 0

    def test_delete_run_persists_to_database(self, platform: ixmp4.Platform) -> None:
        direct = self.get_direct_or_skip(platform.backend.transport)
        assert direct.session.bind is not None
        bind = direct.session.bind
        if bind.engine.dialect.name == "sqlite":
            pytest.skip(
                "SQLite in-memory databases cannot be inspected from a "
                "separate connection."
            )
        engine = sa.create_engine(bind.engine.url)
        try:
            with engine.connect() as conn:
                count = conn.execute(
                    sa.select(sa.func.count()).select_from(Run)
                ).scalar_one()
        finally:
            engine.dispose()
        assert count == 0


class TestRunClone:
    @pytest.fixture(scope="class")
    def units(
        self,
        platform: ixmp4.Platform,
    ) -> list[ixmp4.Unit]:
        return [platform.units.create("Unit 1"), platform.units.create("Unit 2")]

    @pytest.fixture(scope="class")
    def regions(
        self,
        platform: ixmp4.Platform,
    ) -> list[ixmp4.Region]:
        return [
            platform.regions.create("Region 1", "default"),
            platform.regions.create("Region 2", "default"),
        ]

    @pytest.fixture(scope="class")
    def test_data_iamc(
        self,
        regions: list[ixmp4.Region],
        units: list[ixmp4.Unit],
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                ["Region 1", "Unit 1", "Variable 1", 2000, 1.1],
                ["Region 1", "Unit 1", "Variable 1", 2010, 1.3],
                ["Region 1", "Unit 2", "Variable 2", 2020, 1.5],
                ["Region 1", "Unit 2", "Variable 2", 2030, 1.7],
                ["Region 2", "Unit 1", "Variable 1", 2000, 2.1],
                ["Region 2", "Unit 1", "Variable 1", 2010, 2.3],
                ["Region 2", "Unit 2", "Variable 2", 2020, 2.5],
                ["Region 2", "Unit 2", "Variable 2", 2030, 2.7],
            ],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        return df

    @pytest.fixture(scope="class")
    def test_data_meta(self) -> dict[str, Any]:
        return {
            "test_bool": False,
            "test_str": "test",
            "test_int": 13,
            "test_float": 3.14,
        }

    @pytest.fixture(scope="class")
    def test_data_idxset1(
        self,
    ) -> list[str]:
        return ["do", "re", "mi", "fa", "so", "la", "ti"]

    @pytest.fixture(scope="class")
    def test_data_idxset2(
        self,
    ) -> list[float]:
        return [3, 1, 4]

    @pytest.fixture(scope="class")
    def test_data_equation1(self) -> dict[str, list[Any]]:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def test_data_parameter1(self) -> dict[str, list[Any]]:
        return {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def test_data_table1(self) -> dict[str, list[Any]]:
        return {
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def test_data_variable1(self) -> dict[str, list[Any]]:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["so", "la", "ti"],
            "IndexSet 2": [4, 1, 1],
        }

    @pytest.fixture(scope="class")
    def other_platform(self) -> Generator[ixmp4.Platform, None, None]:
        # Build an independent in-memory SQLite database.
        # The engine must be created seperately here to avoid caching it.
        engine = sa.create_engine(
            "sqlite:///:memory:",
            poolclass=sa.StaticPool,
            max_identifier_length=63,
            connect_args={"check_same_thread": False},
        )
        backends.create_model_tables(engine)
        session = sa.orm.Session(bind=engine)
        transport = DirectTransport(session, check_alembic_version=False)
        platform = ixmp4.Platform(Backend(transport))
        # IAMC data requires regions and units to already exist on the
        # destination platform, so create the ones used by the `run` fixture.
        platform.regions.create("Region 1", "default")
        platform.regions.create("Region 2", "default")
        platform.units.create("Unit 1")
        platform.units.create("Unit 2")
        yield platform
        transport.close()

    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
        test_data_iamc: pd.DataFrame,
        test_data_meta: dict[str, Any],
        test_data_idxset1: list[str],
        test_data_idxset2: list[float],
        test_data_equation1: dict[str, list[Any]],
        test_data_parameter1: dict[str, list[Any]],
        test_data_table1: dict[str, list[Any]],
        test_data_variable1: dict[str, list[Any]],
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1

        with run.transact("Add meta indicators"):
            run.meta = test_data_meta

        with run.transact("Add IAMC data"):
            run.iamc.add(test_data_iamc)

        with run.transact("Add Optimization data"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(test_data_idxset1)
            indexset2.add_data(test_data_idxset2)

            run.optimization.scalars.create("Scalar 1", 1.23, "Unit 1")

            equation1 = run.optimization.equations.create(
                "Equation 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            equation1.add_data(test_data_equation1)

            parameter1 = run.optimization.parameters.create(
                "Parameter 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            parameter1.add_data(test_data_parameter1)

            table1 = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            table1.add_data(test_data_table1)

            variable1 = run.optimization.variables.create(
                "Variable 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
            )
            variable1.add_data(test_data_variable1)

        return run

    def _assert_cloned_run(
        self,
        cloned_run: ixmp4.Run,
        run: ixmp4.Run,
        test_data_iamc: pd.DataFrame,
        test_data_meta: dict[str, Any],
        test_data_idxset1: list[str],
        test_data_idxset2: list[float],
        test_data_equation1: dict[str, list[Any]],
        test_data_parameter1: dict[str, list[Any]],
        test_data_table1: dict[str, list[Any]],
        test_data_variable1: dict[str, list[Any]],
    ) -> None:
        assert cloned_run.model.name == run.model.name
        assert cloned_run.scenario.name == run.scenario.name
        assert dict(cloned_run.meta) == test_data_meta

        cloned_df = cloned_run.iamc.tabulate()
        pdt.assert_frame_equal(cloned_df, test_data_iamc, check_like=True)

        indexset1 = cloned_run.optimization.indexsets.get_by_name("IndexSet 1")
        indexset2 = cloned_run.optimization.indexsets.get_by_name("IndexSet 2")
        assert indexset1.data == test_data_idxset1
        assert indexset2.data == test_data_idxset2

        scalar1 = cloned_run.optimization.scalars.get_by_name("Scalar 1")
        assert scalar1.unit.name == "Unit 1"
        assert scalar1.value == 1.23

        equation1 = cloned_run.optimization.equations.get_by_name("Equation 1")
        assert equation1.data == test_data_equation1

        parameter1 = cloned_run.optimization.parameters.get_by_name("Parameter 1")
        assert parameter1.data == test_data_parameter1

        table1 = cloned_run.optimization.tables.get_by_name("Table 1")
        assert table1.data == test_data_table1

        variable1 = cloned_run.optimization.variables.get_by_name("Variable 1")
        assert variable1.data == test_data_variable1

    def test_clone_run(
        self,
        run: ixmp4.Run,
        test_data_iamc: pd.DataFrame,
        test_data_meta: dict[str, Any],
        test_data_idxset1: list[str],
        test_data_idxset2: list[float],
        test_data_equation1: dict[str, list[Any]],
        test_data_parameter1: dict[str, list[Any]],
        test_data_table1: dict[str, list[Any]],
        test_data_variable1: dict[str, list[Any]],
    ) -> None:
        cloned_run = run.clone()

        self._assert_cloned_run(
            cloned_run,
            run,
            test_data_iamc,
            test_data_meta,
            test_data_idxset1,
            test_data_idxset2,
            test_data_equation1,
            test_data_parameter1,
            test_data_table1,
            test_data_variable1,
        )

    def test_clone_run_other_platform(
        self,
        run: ixmp4.Run,
        other_platform: ixmp4.Platform,
        test_data_iamc: pd.DataFrame,
        test_data_meta: dict[str, Any],
        test_data_idxset1: list[str],
        test_data_idxset2: list[float],
        test_data_equation1: dict[str, list[Any]],
        test_data_parameter1: dict[str, list[Any]],
        test_data_table1: dict[str, list[Any]],
        test_data_variable1: dict[str, list[Any]],
    ) -> None:
        cloned_run = run.clone(platform=other_platform)
        assert cloned_run._backend is other_platform.backend
        assert cloned_run.id in [
            other_run.id for other_run in other_platform.runs.list(default_only=False)
        ]

        self._assert_cloned_run(
            cloned_run,
            run,
            test_data_iamc,
            test_data_meta,
            test_data_idxset1,
            test_data_idxset2,
            test_data_equation1,
            test_data_parameter1,
            test_data_table1,
            test_data_variable1,
        )

        backend_clone = run.clone(platform=other_platform.backend)
        assert backend_clone._backend is other_platform.backend

        self._assert_cloned_run(
            backend_clone,
            run,
            test_data_iamc,
            test_data_meta,
            test_data_idxset1,
            test_data_idxset2,
            test_data_equation1,
            test_data_parameter1,
            test_data_table1,
            test_data_variable1,
        )

    def test_clone_failure_deletes_dirty_destination_run(
        self,
        run: ixmp4.Run,
        other_platform: ixmp4.Platform,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def boom_clone(
            self: RunCloner, src_run: ixmp4.Run, dst_run: ixmp4.Run, keep_solution: bool
        ) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr(RunCloner, "clone", boom_clone)

        before = len(other_platform.runs.list(default_only=False))

        with pytest.raises(RuntimeError, match="boom"):
            run.clone(platform=other_platform)

        assert len(other_platform.runs.list(default_only=False)) == before

    def test_clone_failure_warns_when_dirty_run_cannot_be_deleted(
        self,
        run: ixmp4.Run,
        other_platform: ixmp4.Platform,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def boom_clone(
            self: RunCloner, src_run: ixmp4.Run, dst_run: ixmp4.Run, keep_solution: bool
        ) -> None:
            raise RuntimeError("boom")

        def forbidden_delete(id: int) -> None:
            raise Forbidden()

        monkeypatch.setattr(RunCloner, "clone", boom_clone)
        monkeypatch.setattr(
            other_platform.backend.runs, "delete_by_id", forbidden_delete
        )

        before = len(other_platform.runs.list(default_only=False))

        with caplog.at_level(logging.WARNING, logger="ixmp4.core.run"):
            with pytest.raises(RuntimeError, match="boom"):
                run.clone(platform=other_platform)

        assert any(
            "leaving a dirty destination run" in record.message
            for record in caplog.records
        )
        assert len(other_platform.runs.list(default_only=False)) == before + 1

    @pytest.fixture()
    def empty_platform(self) -> Generator[ixmp4.Platform, None, None]:
        engine = sa.create_engine(
            "sqlite:///:memory:",
            poolclass=sa.StaticPool,
            max_identifier_length=63,
            connect_args={"check_same_thread": False},
        )
        backends.create_model_tables(engine)
        session = sa.orm.Session(bind=engine)
        transport = DirectTransport(session, check_alembic_version=False)
        platform = ixmp4.Platform(Backend(transport))
        yield platform
        transport.close()

    def test_clone_missing_region_raises(
        self,
        run: ixmp4.Run,
        empty_platform: ixmp4.Platform,
    ) -> None:
        with pytest.raises(RegionNotFound):
            run.clone(platform=empty_platform)

    def test_clone_missing_unit_from_iamc_raises(
        self,
        platform: ixmp4.Platform,
        empty_platform: ixmp4.Platform,
    ) -> None:
        platform.units.create("Unit X")
        run = platform.runs.create("Model", "UnitTest")
        df = pd.DataFrame(
            [["Region 1", "Unit X", "Variable 1", 2000, 1.1]],
            columns=["region", "unit", "variable", "year", "value"],
        )
        df["year"] = df["year"].astype("Int64")
        with run.transact("Add IAMC data"):
            run.iamc.add(df)
        empty_platform.regions.create("Region 1", "default")
        with pytest.raises(UnitNotFound):
            run.clone(platform=empty_platform)

    def test_clone_missing_unit_from_optimization_scalar_raises(
        self,
        platform: ixmp4.Platform,
        empty_platform: ixmp4.Platform,
    ) -> None:
        platform.units.create("Unit Y")
        run = platform.runs.create("Model", "ScalarUnitTest")
        with run.transact("Add scalar"):
            run.optimization.scalars.create("Scalar X", 42.0, "Unit Y")
        with pytest.raises(UnitNotFound):
            run.clone(platform=empty_platform)

    def test_clone_missing_unit_from_optimization_parameter_raises(
        self,
        platform: ixmp4.Platform,
        empty_platform: ixmp4.Platform,
    ) -> None:
        platform.units.create("Unit Z")
        run = platform.runs.create("Model", "ParamUnitTest")
        with run.transact("Add parameter data"):
            idxset = run.optimization.indexsets.create("IndexSet 1")
            idxset.add_data(["a", "b"])
            parameter = run.optimization.parameters.create(
                "Parameter X", constrained_to_indexsets=["IndexSet 1"]
            )
            parameter.add_data(
                {
                    "units": ["Unit Z", "Unit Z"],
                    "values": [1.0, 2.0],
                    "IndexSet 1": ["a", "b"],
                }
            )
        with pytest.raises(UnitNotFound):
            run.clone(platform=empty_platform)

    def test_clone_no_validation_when_same_platform(
        self,
        run: ixmp4.Run,
    ) -> None:
        cloned_run = run.clone()
        assert cloned_run.model.name == run.model.name
