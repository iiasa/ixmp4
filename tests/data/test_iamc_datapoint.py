import pandas as pd
import pytest

from ixmp4.core.exceptions import BadFilterArguments

from ..utils import add_regions, add_units, all_platforms, assert_unordered_equality


@all_platforms
@pytest.mark.parametrize(
    "filter,exp_filter",
    [
        ({"year": 2005}, ("step_year", "__eq__", 2005)),
        ({"year__in": [2005, 2010]}, ("step_year", "isin", [2005, 2010])),
        ({"region": {"name__in": ["World"]}}, ("region", "__eq__", "World")),
        ({"region": {"hierarchy": "default"}}, ("region", "isin", ["World", "Europe"])),
        ({"unit": {"name": "EJ/yr"}}, ("unit", "__eq__", "EJ/yr")),
        ({"unit": {"name": "%"}}, ("unit", "__eq__", "%")),
        ({"unit": {"name__like": "*"}}, ("unit", "isin", ["EJ/yr", "%"])),
        ({"unit": {"name__like": "%"}}, ("unit", "__eq__", "%")),
        (
            {"variable": {"name__in": ["Primary Energy"]}},
            ("variable", "isin", ["Primary Energy"]),
        ),
        ({"model": {"name": "model_1"}}, ("region", "__eq__", "World")),
        ({"model": {"name": "model_2"}}, ("region", "__eq__", "Europe")),
        ({"scenario": {"name__in": ["scen_1", "scen_2"]}}, None),
    ],
)
def test_filtering(test_mp, filter, exp_filter, request):
    test_mp = request.getfixturevalue(test_mp)
    # preparing the data
    test_data_columns = ["region", "variable", "unit", "step_year", "value"]
    test_data = [
        pd.DataFrame(
            [
                ["World", "Primary Energy", "EJ/yr", 2005, 1.0],
            ],
            columns=test_data_columns,
        ),
        pd.DataFrame(
            [
                ["Europe", "Efficiency", "%", 2010, 60.0],
            ],
            columns=test_data_columns,
        ),
    ]

    for i, data in enumerate(test_data):
        data["type"] = "ANNUAL"
        add_regions(test_mp, data.region.unique())
        add_units(test_mp, data.unit.unique())
        # add the data for two different models to test filtering
        run = test_mp.runs.create(f"model_{i + 1}", f"scen_{i + 1}")
        run.iamc.add(data)
        run.set_as_default()

    obs = (
        test_mp.backend.iamc.datapoints.tabulate(join_parameters=True, **filter)
        .drop(["id", "time_series__id"], axis="columns")
        .sort_index(axis=1)
    )

    exp = pd.concat(test_data)
    if exp_filter is not None:
        exp = exp[getattr(exp[exp_filter[0]], exp_filter[1])(exp_filter[2])]
    assert_unordered_equality(obs, exp, check_like=True)


@all_platforms
@pytest.mark.parametrize(
    "filter",
    [
        {"dne": {"dne": "test"}},
        {"region": {"dne": "test"}},
        {"region": {"name__in": False}},
        {"run": {"default_only": "test"}},
    ],
)
def test_invalid_filters(test_mp, filter, request):
    test_mp = request.getfixturevalue(test_mp)
    with pytest.raises(BadFilterArguments):
        test_mp.backend.iamc.datapoints.tabulate(**filter)
    with pytest.raises(BadFilterArguments):
        test_mp.backend.iamc.datapoints.list(**filter)
