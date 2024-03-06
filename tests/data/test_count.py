from functools import reduce

import pytest

from ..utils import generated_db_platforms


def deepgetattr(obj, attr):
    return reduce(getattr, attr.split("."), obj)


@generated_db_platforms
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
def test_count(generated_mp, repo_name, filters, request):
    generated_mp = request.getfixturevalue(generated_mp)
    repo = deepgetattr(generated_mp.backend, repo_name)
    assert len(repo.list(**filters)) == repo.count(**filters)
