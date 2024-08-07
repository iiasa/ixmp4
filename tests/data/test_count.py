from functools import reduce

import pytest

import ixmp4

from ..fixtures import BigIamcDataset


def deepgetattr(obj, attr):
    return reduce(getattr, attr.split("."), obj)


big = BigIamcDataset()


@pytest.mark.parametrize(
    "repo_name,filters",
    [
        [
            "iamc.datapoints",
            {
                "model": {"name": "Model 0"},
                "scenario": {"name": "Scenario 0"},
                "run": {"default_only": False},
            },
        ],
        [
            "iamc.datapoints",
            {
                "scenario": {"name__like": "Scenario *"},
                "run": {"default_only": False},
            },
        ],
        [
            "iamc.datapoints",
            {
                "model": {"name__like": "Model *"},
                "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
                "variable": {"name__like": "Variable 1*"},
                "region": {"name__in": [f"Region {i}" for i in range(10)]},
                "run": {"default_only": False},
            },
        ],
        [
            "regions",
            {
                "name__like": "Region 1*",
                "iamc": {
                    "run": {"default_only": False},
                },
            },
        ],
        [
            "models",
            {
                "name__like": "Model *",
                "iamc": True,
            },
        ],
        [
            "scenarios",
            {
                "name__like": "Scenario *",
                "iamc": {
                    "variable": {"name__like": "Variable *"},
                    "run": {"default_only": False},
                },
            },
        ],
        [
            "units",
            {
                "iamc": {
                    "variable": {"name__like": "Variable *"},
                    "run": {"default_only": False},
                },
            },
        ],
    ],
)
def test_count(db_platform: ixmp4.Platform, repo_name, filters):
    big.load_dataset(db_platform)
    repo = deepgetattr(db_platform.backend, repo_name)
    assert len(repo.list(**filters)) == repo.count(**filters)
