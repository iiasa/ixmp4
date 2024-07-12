import pytest

import ixmp4
from ixmp4 import Variable

from ..utils import create_filter_test_data


class TestDataIamcVariable:
    def test_create_iamc_variable(self, platform: ixmp4.Platform):
        variable = platform.backend.iamc.variables.create("Variable")
        assert variable.name == "Variable"
        assert variable.created_at is not None
        assert variable.created_by == "@unknown"

    def test_iamc_variable_unique(self, platform: ixmp4.Platform):
        platform.backend.iamc.variables.create("Variable")

        with pytest.raises(Variable.NotUnique):
            platform.iamc.variables.create("Variable")

    def test_iamc_variable_not_found(self, platform: ixmp4.Platform):
        with pytest.raises(Variable.NotFound):
            platform.iamc.variables.get("Variable")

    def test_filter_iamc_variable(self, platform: ixmp4.Platform):
        run1, run2 = create_filter_test_data(platform)
        res = platform.backend.iamc.variables.tabulate(unit={"name": "Unit 1"})
        assert sorted(res["name"].tolist()) == ["Variable 1", "Variable 2"]

        run2.set_as_default()
        res = platform.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"}, region={"name": "Region 3"}
        )
        assert sorted(res["name"].tolist()) == ["Variable 1"]

        run1.set_as_default()
        res = platform.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"},
            region={"name__in": ["Region 4", "Region 5"]},
            run={"model": {"name": "Model 1"}, "default_only": True},
        )
        assert res["name"].tolist() == []

        res = platform.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"},
            region={"name__in": ["Region 4", "Region 5"]},
            run={"default_only": False},
        )
        assert sorted(res["name"].tolist()) == ["Variable 2"]

        res = platform.backend.iamc.variables.tabulate()

        assert sorted(res["name"].tolist()) == [
            "Variable 1",
            "Variable 2",
        ]
