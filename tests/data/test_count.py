from functools import reduce
from typing import Any

import pytest

import ixmp4
from ixmp4.data.backend import Backend
from ixmp4.data.db.base import CountKwargs, Enumerator


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
        self, db_platform_big: ixmp4.Platform, repo_name: str, filters: CountKwargs
    ) -> None:
        repo = deepgetattr(db_platform_big.backend, repo_name)
        # NOTE this check would not be necessary if db.platform_big.backend was typed as
        # a DB backend and deepgetattr() to return DB-layer Enumerator
        assert isinstance(repo, Enumerator)
        assert len(repo.list(**filters)) == repo.count(**filters)
