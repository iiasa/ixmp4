from functools import reduce
from typing import Any

import pytest

import ixmp4
from ixmp4.data.backend import Backend


# TODO How to type hint this to return Repos with .list() and .count()?
def deepgetattr(obj: Backend, attr: str) -> Any:
    return reduce(getattr, attr.split("."), obj)


class TestDataCount:
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
    def test_count(
        self,
        db_platform_big: ixmp4.Platform,
        repo_name: str,
        filters: dict[
            str, bool | str | dict[str, bool | str | list[str] | dict[str, bool | str]]
        ],
    ) -> None:
        repo = deepgetattr(db_platform_big.backend, repo_name)
        assert len(repo.list(**filters)) == repo.count(**filters)
