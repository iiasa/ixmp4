import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4.core import Run
from ixmp4.core.exceptions import IxmpError

from ..fixtures import MediumIamcDataset


def _expected_runs_table(*row_default):
    rows = []
    for i, default in enumerate(row_default, start=1):
        if default is not None:
            rows.append([i, "Model", "Scenario", i] + [default])

    return pd.DataFrame(
        rows, columns=["id", "model", "scenario", "version", "is_default"]
    )


class TestCoreRun:
    medium = MediumIamcDataset()

    def test_run_notfound(self, platform: ixmp4.Platform):
        # no Run with that model and scenario name exists
        with pytest.raises(Run.NotFound):
            _ = platform.runs.get("Unknown Model", "Unknown Scenario", version=1)

    def test_run_versions(self, platform: ixmp4.Platform):
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

        self.medium.load_dataset(platform)
        all_runs = self.medium.runs.copy()

        res = platform.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 1"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 0"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = platform.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 3"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 0"]

        res = platform.runs.tabulate(
            iamc={
                "variable": {"name__like": "Variable 10*"},
                "unit": {"name__in": ["Unit 10", "Unit 2"]},
            }
        )
        assert sorted(res["model"].tolist()) == ["Model 3", "Model 4"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 0", "Scenario 1"]

        res = platform.runs.tabulate(
            default_only=False,
            scenario={"name__in": ["Scenario 2", "Scenario 3"]},
            iamc=None,
        )
        exp_runs = all_runs[all_runs["scenario"].isin(["Scenario 2", "Scenario 3"])]

        assert sorted(res["model"].tolist()) == sorted(exp_runs["model"].tolist())
        assert sorted(res["scenario"].tolist()) == sorted(exp_runs["scenario"].tolist())

        res = platform.runs.tabulate(
            default_only=False,
            iamc=False,
        )
        assert sorted(res["model"].tolist()) == ["Model", "Model"]
        assert sorted(res["scenario"].tolist()) == ["Scenario", "Scenario"]

        for run in platform.runs.list(
            default_only=False,
            model={"name": "Model 1"},
            scenario={"name": "Scenario 0"},
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

    def delete_all_datapoints(self, run: ixmp4.Run):
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

        run.iamc.remove(annual, type=ixmp4.DataPoint.Type.ANNUAL)
        run.iamc.remove(cat, type=ixmp4.DataPoint.Type.CATEGORICAL)
        run.iamc.remove(datetime, type=ixmp4.DataPoint.Type.DATETIME)
