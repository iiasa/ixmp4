import pytest

import ixmp4
from ixmp4.core.exceptions import BadFilterArguments

from ..fixtures import FilterIamcDataset
from ..utils import assert_unordered_equality

filter_dataset = FilterIamcDataset()


@pytest.mark.parametrize(
    "filter,exp_filter",
    [
        ({"year": 2020}, ("step_year", "__eq__", 2020)),
        ({"year__in": [2000, 2010]}, ("step_year", "isin", [2000, 2010])),
        ({"region": {"name__in": ["Region 1"]}}, ("region", "__eq__", "Region 1")),
        (
            {"region": {"hierarchy": "Hierarchy"}},
            ("region", "isin", ["Region 1", "Region 2", "Region 4"]),
        ),
        ({"unit": {"name": "Unit 1"}}, ("unit", "__eq__", "Unit 1")),
        ({"unit": {"name": "Unit 2"}}, ("unit", "__eq__", "Unit 2")),
        (
            {"unit": {"name__like": "*"}},
            ("unit", "isin", ["Unit 1", "Unit 2", "Unit 3", "Unit 4"]),
        ),
        ({"unit": {"name__like": "Unit 3"}}, ("unit", "__eq__", "Unit 3")),
        (
            {"variable": {"name__in": ["Variable 1"]}},
            ("variable", "isin", ["Variable 1"]),
        ),
        ({"model": {"name": "Model 1"}}, ("model", "__eq__", "Model 1")),
        ({"scenario": {"name": "Scenario 2"}}, ("scenario", "__eq__", "Scenario 2")),
    ],
)
def test_filtering(platform: ixmp4.Platform, filter, exp_filter):
    run1, run2 = filter_dataset.load_dataset(platform)
    run2.set_as_default()
    obs = platform.backend.iamc.datapoints.tabulate(join_parameters=True, **filter)

    exp = filter_dataset.datapoints.copy()
    if exp_filter is not None:
        exp = exp[getattr(exp[exp_filter[0]], exp_filter[1])(exp_filter[2])]

    exp = exp.drop(columns=["model", "scenario"])
    if not obs.empty:
        obs = obs.drop(["id", "time_series__id", "type"], axis="columns")
        obs = obs.sort_index(axis=1)
        assert_unordered_equality(obs, exp, check_like=True)
    else:
        assert exp.empty


@pytest.mark.parametrize(
    "filter",
    [
        {"dne": {"dne": "test"}},
        {"region": {"dne": "test"}},
        {"region": {"name__in": False}},
        {"run": {"default_only": "test"}},
    ],
)
def test_invalid_filters(platform: ixmp4.Platform, filter, request):
    with pytest.raises(BadFilterArguments):
        platform.backend.iamc.datapoints.tabulate(**filter)
    with pytest.raises(BadFilterArguments):
        platform.backend.iamc.datapoints.list(**filter)
