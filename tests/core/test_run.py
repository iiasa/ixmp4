import pandas as pd
import pandas.testing as pdt
import pytest

# Import this from typing when dropping 3.11
from typing_extensions import Unpack

import ixmp4
from ixmp4.core import Run
from ixmp4.core.exceptions import IxmpError, RunLockRequired

from ..fixtures import FilterIamcDataset, SmallIamcDataset


def _expected_runs_table(*row_default: Unpack[tuple[bool | None, ...]]) -> pd.DataFrame:
    rows = []
    for i, default in enumerate(row_default, start=1):
        if default is not None:
            rows.append(["Model", "Scenario", i] + [default])

    return pd.DataFrame(rows, columns=["model", "scenario", "version", "is_default"])


class TestCoreRun:
    filter = FilterIamcDataset()
    small = SmallIamcDataset()

    def test_run_notfound(self, platform: ixmp4.Platform) -> None:
        # no Run with that model and scenario name exists
        with pytest.raises(Run.NotFound):
            _ = platform.runs.get("Unknown Model", "Unknown Scenario", version=1)

    def test_run_versions(self, platform: ixmp4.Platform) -> None:
        run1 = platform.runs.create("Model", "Scenario")
        run2 = platform.runs.create("Model", "Scenario")

        assert run1.id != run2.id

        # no default version is assigned, so list & tabulate are empty
        with pytest.raises(Run.NoDefaultVersion):
            _ = platform.runs.get("Model", "Scenario")
        assert platform.runs.list() == []
        assert platform.runs.tabulate().empty

        # getting a specific version works even if no default version is assigned
        assert run1.id == platform.runs.get("Model", "Scenario", version=1).id

        # getting the table and list for all runs works
        run_list = platform.runs.list(default_only=False)
        assert len(run_list) == 2
        assert run_list[0].id == run1.id
        pdt.assert_frame_equal(
            platform.runs.tabulate(default_only=False),
            pd.DataFrame(_expected_runs_table(False, False)),
        )

        # set default, so list & tabulate show default version only
        run1.set_as_default()
        run_list = platform.runs.list()
        assert len(run_list) == 1
        assert run_list[0].id == run1.id
        pdt.assert_frame_equal(
            platform.runs.tabulate(),
            pd.DataFrame(_expected_runs_table(True)),
        )

        # using default_only=False shows both versions
        pdt.assert_frame_equal(
            platform.runs.tabulate(default_only=False),
            pd.DataFrame(_expected_runs_table(True, False)),
        )

        # using audit_info=True shows additional columns
        audit_info = platform.runs.tabulate(default_only=False, audit_info=True)
        for column in ["updated_at", "updated_by", "created_at", "created_by", "id"]:
            assert column in audit_info.columns
        pdt.assert_series_equal(audit_info.id, pd.Series([run1.id, run2.id], name="id"))

        # default version can be retrieved directly
        run = platform.runs.get("Model", "Scenario")
        assert run1.id == run.id

        # default version can be changed
        run2.set_as_default()
        run = platform.runs.get("Model", "Scenario")
        assert run2.id == run.id

        # list shows changed default version only
        run_list = platform.runs.list()
        assert len(run_list) == 1
        assert run_list[0].id == run2.id
        pdt.assert_frame_equal(
            platform.runs.tabulate(),
            pd.DataFrame(_expected_runs_table(None, True)),
        )

        # unsetting default means run cannot be retrieved directly
        run2.unset_as_default()
        with pytest.raises(Run.NoDefaultVersion):
            platform.runs.get("Model", "Scenario")

        # non-default version cannot be again set as un-default
        with pytest.raises(IxmpError):
            run2.unset_as_default()

        self.filter.load_dataset(platform)

        res = platform.runs.tabulate(
            iamc={
                "region": {"name": "Region 1"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = platform.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 2"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2"]

        res = platform.runs.tabulate(
            default_only=True,
            iamc={
                "variable": {"name__like": "Variable *"},
                "unit": {"name__in": ["Unit 2", "Unit 4"]},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = platform.runs.tabulate(
            default_only=False,
            scenario={"name__in": ["Scenario 1", "Scenario 2"]},
            iamc=None,
        )

        assert sorted(res["model"].tolist()) == ["Model 1", "Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1", "Scenario 2"]

        res = platform.runs.tabulate(
            default_only=False,
            iamc=False,
        )
        assert sorted(res["model"].tolist()) == ["Model", "Model"]
        assert sorted(res["scenario"].tolist()) == ["Scenario", "Scenario"]

        for run in platform.runs.list(
            default_only=False,
            model={"name": "Model 1"},
            scenario={"name": "Scenario 1"},
        ):
            self.delete_all_datapoints(run)
        res = platform.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 3"},
            },
        )
        assert sorted(res["model"].tolist()) == []
        assert sorted(res["scenario"].tolist()) == []

    def delete_all_datapoints(self, run: ixmp4.Run) -> None:
        remove_data = run.iamc.tabulate(raw=True)
        annual = remove_data[remove_data["type"] == "ANNUAL"].dropna(
            how="all", axis="columns"
        )
        cat = remove_data[remove_data["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )

        datetime = remove_data[remove_data["type"] == "DATETIME"].dropna(
            how="all", axis="columns"
        )
        with run.transact("Remove iamc data"):
            if not annual.empty:
                run.iamc.remove(annual, type=ixmp4.DataPoint.Type.ANNUAL)
            if not cat.empty:
                run.iamc.remove(cat, type=ixmp4.DataPoint.Type.CATEGORICAL)
            if not datetime.empty:
                run.iamc.remove(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    def test_run_remove_solution(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(["foo", "bar"])
        test_data = {
            "Indexset": ["bar", "foo"],
            "levels": [2.0, 1],
            "marginals": [0, "test"],
        }
        run.optimization.equations.create(
            "Equation",
            constrained_to_indexsets=[indexset.name],
        ).add(test_data)
        run.optimization.variables.create(
            "Variable",
            constrained_to_indexsets=[indexset.name],
        ).add(test_data)

        run.optimization.remove_solution()
        # Need to fetch them here even if fetched before because API layer might not
        # forward changes automatically
        equation = run.optimization.equations.get("Equation")
        variable = run.optimization.variables.get("Variable")
        assert equation.data == {}
        assert variable.data == {}

    def test_run_delete_via_object_method(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1 = platform.runs.get("Model 1", "Scenario 1")
        run2 = platform.runs.get("Model 2", "Scenario 2")

        for run in [run1, run2]:
            with pytest.raises(RunLockRequired):
                run.delete()

            with run.transact("Delete run"):
                run.delete()

            self.assert_run_data_deleted(platform, run)

    def test_run_delete_via_repository_id(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1 = platform.runs.get("Model 1", "Scenario 1")
        run2 = platform.runs.get("Model 2", "Scenario 2")

        for run in [run1, run2]:
            platform.runs.delete(run.id)
            self.assert_run_data_deleted(platform, run)

    def test_run_delete_via_repository_object(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1 = platform.runs.get("Model 1", "Scenario 1")
        run2 = platform.runs.get("Model 2", "Scenario 2")

        for run in [run1, run2]:
            platform.runs.delete(run)
            self.assert_run_data_deleted(platform, run)

    def test_run_invalid_argument(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(TypeError):
            platform.runs.delete("Model|Scenario")  # type: ignore[arg-type]

    def assert_run_data_deleted(self, platform: ixmp4.Platform, run: Run) -> None:
        ret_meta = platform.backend.meta.tabulate(
            run={"id": run.id, "default_only": False}
        )
        assert ret_meta.empty

        ret_iamc_dps = platform.backend.iamc.datapoints.tabulate(
            run={"id": run.id, "default_only": True}
        )
        assert ret_iamc_dps.empty

        # TODO: check if optimization data is deleted. @glatterf42
