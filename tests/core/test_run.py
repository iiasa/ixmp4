import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4 import IxmpError, Run

from ..utils import all_platforms, create_filter_test_data


def _expected_runs_table(*row_default):
    rows = []
    for i, default in enumerate(row_default, start=1):
        if default is not None:
            rows.append([i, "Model", "Scenario", i] + [default])

    return pd.DataFrame(
        rows, columns=["id", "model", "scenario", "version", "is_default"]
    )


@all_platforms
class TestCoreRun:
    def test_run_notfound(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        # no Run with that model and scenario name exists
        with pytest.raises(Run.NotFound):
            _ = test_mp.runs.get("Unknown Model", "Unknown Scenario", version=1)

    def test_run_versions(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run1 = test_mp.runs.create("Model", "Scenario")
        run2 = test_mp.runs.create("Model", "Scenario")

        assert run1.id != run2.id

        # no default version is assigned, so list & tabulate are empty
        with pytest.raises(Run.NoDefaultVersion):
            _ = test_mp.runs.get("Model", "Scenario")
        assert test_mp.runs.list() == []
        assert test_mp.runs.tabulate().empty

        # getting a specific version works even if no default version is assigned
        assert run1.id == test_mp.runs.get("Model", "Scenario", version=1).id

        # getting the table and list for all runs works
        run_list = test_mp.runs.list(default_only=False)
        assert len(run_list) == 2
        assert run_list[0].id == run1.id
        pdt.assert_frame_equal(
            test_mp.runs.tabulate(default_only=False),
            pd.DataFrame(_expected_runs_table(False, False)),
        )

        # set default, so list & tabulate show default version only
        run1.set_as_default()
        run_list = test_mp.runs.list()
        assert len(run_list) == 1
        assert run_list[0].id == run1.id
        pdt.assert_frame_equal(
            test_mp.runs.tabulate(),
            pd.DataFrame(_expected_runs_table(True)),
        )

        # using default_only=False shows both versions
        pdt.assert_frame_equal(
            test_mp.runs.tabulate(default_only=False),
            pd.DataFrame(_expected_runs_table(True, False)),
        )

        # default version can be retrieved directly
        run = test_mp.runs.get("Model", "Scenario")
        assert run1.id == run.id

        # default version can be changed
        run2.set_as_default()
        run = test_mp.runs.get("Model", "Scenario")
        assert run2.id == run.id

        # list shows changed default version only
        run_list = test_mp.runs.list()
        assert len(run_list) == 1
        assert run_list[0].id == run2.id
        pdt.assert_frame_equal(
            test_mp.runs.tabulate(),
            pd.DataFrame(_expected_runs_table(None, True)),
        )

        # unsetting default means run cannot be retrieved directly
        run2.unset_as_default()
        with pytest.raises(Run.NoDefaultVersion):
            test_mp.runs.get("Model", "Scenario")

        # non-default version cannot be again set as un-default
        with pytest.raises(IxmpError):
            run2.unset_as_default()

    def test_filter_run(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run1, _ = create_filter_test_data(test_mp)

        res = test_mp.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 1"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = test_mp.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 3"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1", "Model 1", "Model 2"]
        assert sorted(res["scenario"].tolist()) == [
            "Scenario 1",
            "Scenario 1",
            "Scenario 2",
        ]

        run1.set_as_default()
        res = test_mp.runs.tabulate(
            iamc={
                "variable": {"name": "Variable 1"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
            }
        )
        assert sorted(res["model"].tolist()) == ["Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2"]

        res = test_mp.runs.tabulate(
            default_only=False,
            scenario={"name": "Scenario 2"},
        )

        assert sorted(res["model"].tolist()) == ["Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2"]
