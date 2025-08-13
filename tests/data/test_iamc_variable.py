import pandas as pd
import pytest

import ixmp4
from ixmp4 import Variable
from ixmp4.data.backend import SqlAlchemyBackend
from ixmp4.data.db.versions import Operation

from .. import utils
from ..fixtures import FilterIamcDataset


class TestDataIamcVariable:
    filter = FilterIamcDataset()

    def test_create_iamc_variable(self, platform: ixmp4.Platform) -> None:
        variable = platform.backend.iamc.variables.create("Variable")
        assert variable.name == "Variable"
        assert variable.created_at is not None
        assert variable.created_by == "@unknown"

        @utils.versioning_test(platform.backend)
        def assert_versions(backend: SqlAlchemyBackend) -> None:
            expected_versions = pd.DataFrame(
                [
                    [
                        1,
                        "Variable",
                        variable.created_at,
                        "@unknown",
                        1,
                        None,
                        Operation.INSERT,
                    ],
                ],
                columns=[
                    "id",
                    "name",
                    "created_at",
                    "created_by",
                    "transaction_id",
                    "end_transaction_id",
                    "operation_type",
                ],
            )
            vdf = backend.iamc.variables.versions.tabulate()
            utils.assert_unordered_equality(expected_versions, vdf)

    def test_iamc_variable_unique(self, platform: ixmp4.Platform) -> None:
        platform.backend.iamc.variables.create("Variable")

        with pytest.raises(Variable.NotUnique):
            platform.iamc.variables.create("Variable")

    def test_iamc_variable_not_found(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(Variable.NotFound):
            platform.iamc.variables.get("Variable")

    def test_filter_iamc_variable(self, platform: ixmp4.Platform) -> None:
        run1, run2 = self.filter.load_dataset(platform)
        res = platform.backend.iamc.variables.tabulate(unit={"name": "Unit 1"})
        assert sorted(res["name"].tolist()) == ["Variable 1", "Variable 3"]

        run2.set_as_default()
        res = platform.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"}, region={"name": "Region 4"}
        )
        assert sorted(res["name"].tolist()) == ["Variable 7"]

        run2.unset_as_default()
        res = platform.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"},
            region={"name__in": ["Region 2", "Region 4"]},
            run={"model": {"name": "Model 2"}, "default_only": True},
        )
        assert res["name"].tolist() == []

        res = platform.backend.iamc.variables.tabulate(
            unit={"name": "Unit 3"},
            region={"name__in": ["Region 2", "Region 4"]},
            run={"model": {"name": "Model 2"}, "default_only": False},
        )
        assert sorted(res["name"].tolist()) == ["Variable 5", "Variable 7"]

        res = platform.backend.iamc.variables.tabulate()

        assert sorted(res["name"].tolist()) == [
            "Variable 1",
            "Variable 3",
            "Variable 4",
        ]
