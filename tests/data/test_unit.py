import pandas as pd
import pytest

import ixmp4
from ixmp4 import Unit
from ixmp4.data.backend import SqlAlchemyBackend

from .. import utils
from ..fixtures import FilterIamcDataset


class TestDataUnit:
    filter = FilterIamcDataset()

    def test_create_get_unit(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.backend.units.create("Unit")
        assert unit1.name == "Unit"

        @utils.versioning_test(platform.backend)
        def assert_versions(backend: SqlAlchemyBackend) -> None:
            expected_versions = pd.DataFrame(
                [
                    [1, "Unit", unit1.created_at, "@unknown", 1, None, 0],
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
            vdf = backend.units.versions.tabulate()
            utils.assert_unordered_equality(expected_versions, vdf, check_dtype=False)

        unit2 = platform.backend.units.get("Unit")
        assert unit1.id == unit2.id

    def test_delete_unit(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.backend.units.create("Unit")
        platform.backend.units.delete(unit1.id)
        assert platform.backend.units.tabulate().empty

    def test_get_or_create_unit(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.backend.units.create("Unit")
        unit2 = platform.backend.units.get_or_create("Unit")
        assert unit1.id == unit2.id

        unit3 = platform.backend.units.get_or_create("Another Unit")
        assert unit3.name == "Another Unit"
        assert unit1.id != unit3.id

    def test_unit_unique(self, platform: ixmp4.Platform) -> None:
        platform.backend.units.create("Unit")

        with pytest.raises(Unit.NotUnique):
            platform.backend.units.create("Unit")

    def test_unit_not_found(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(Unit.NotFound):
            platform.backend.units.get("Unit")

    def test_list_unit(self, platform: ixmp4.Platform) -> None:
        platform.backend.units.create("Unit 1")
        platform.backend.units.create("Unit 2")

        units = platform.backend.units.list()
        units = sorted(units, key=lambda x: x.id)

        assert units[0].id == 1
        assert units[0].name == "Unit 1"
        assert units[1].id == 2
        assert units[1].name == "Unit 2"

    def test_tabulate_unit(self, platform: ixmp4.Platform) -> None:
        platform.backend.units.create("Unit 1")
        platform.backend.units.create("Unit 2")

        true_units = pd.DataFrame(
            [
                [1, "Unit 1"],
                [2, "Unit 2"],
            ],
            columns=["id", "name"],
        )

        units = platform.backend.units.tabulate()
        utils.assert_unordered_equality(
            units.drop(columns=["created_at", "created_by"]), true_units
        )

    def test_filter_unit(self, platform: ixmp4.Platform) -> None:
        run1, run2 = self.filter.load_dataset(platform)
        res = platform.backend.units.tabulate(
            iamc={
                "variable": {"name": "Variable 1"},
            }
        )
        assert sorted(res["name"].tolist()) == ["Unit 1", "Unit 2"]

        run2.set_as_default()
        res = platform.backend.units.tabulate(
            iamc={
                "run": {"model": {"name": "Model 2"}},
            }
        )
        assert sorted(res["name"].tolist()) == ["Unit 3", "Unit 4"]

        run2.unset_as_default()
        res = platform.backend.units.tabulate(
            iamc={
                "variable": {"name": "Variable 6"},
                "region": {"name__in": ["Region 2", "Region 4"]},
                "run": {"model": {"name": "Model 2"}, "default_only": True},
            }
        )
        assert res["name"].tolist() == []

        res = platform.backend.units.tabulate(
            iamc={
                "variable": {"name": "Variable 6"},
                "region": {"name__in": ["Region 2", "Region 4"]},
                "run": {"model": {"name": "Model 2"}, "default_only": False},
            }
        )
        assert sorted(res["name"].tolist()) == ["Unit 4"]

        res = platform.backend.units.tabulate(iamc=False)

        assert res["name"].tolist() == ["Unit 5"]

        res = platform.backend.units.tabulate()

        assert sorted(res["name"].tolist()) == [
            "Unit 1",
            "Unit 2",
            "Unit 3",
            "Unit 4",
            "Unit 5",
        ]
