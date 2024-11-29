import pandas as pd
import pytest

import ixmp4
from ixmp4 import Region

from ..fixtures import FilterIamcDataset
from ..utils import assert_unordered_equality


class TestDataRegion:
    filter = FilterIamcDataset()

    def test_create_region(self, platform: ixmp4.Platform) -> None:
        region1 = platform.backend.regions.create("Region", "Hierarchy")
        assert region1.name == "Region"
        assert region1.hierarchy == "Hierarchy"
        assert region1.created_at is not None
        assert region1.created_by == "@unknown"

    def test_delete_region(self, platform: ixmp4.Platform) -> None:
        region1 = platform.backend.regions.create("Region", "Hierarchy")
        platform.backend.regions.delete(region1.id)
        assert platform.backend.regions.tabulate().empty

    def test_region_unique(self, platform: ixmp4.Platform) -> None:
        platform.backend.regions.create("Region", "Hierarchy")

        with pytest.raises(Region.NotUnique):
            platform.regions.create("Region", "Hierarchy")

        with pytest.raises(Region.NotUnique):
            platform.regions.create("Region", "Another Hierarchy")

    def test_get_region(self, platform: ixmp4.Platform) -> None:
        region1 = platform.backend.regions.create("Region", "Hierarchy")
        region2 = platform.backend.regions.get("Region")
        assert region1 == region2

    def test_region_not_found(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(Region.NotFound):
            platform.regions.get("Region")

    def test_get_or_create_region(self, platform: ixmp4.Platform) -> None:
        region1 = platform.backend.regions.create("Region", "Hierarchy")
        region2 = platform.backend.regions.get_or_create("Region")
        assert region1.id == region2.id

        platform.backend.regions.get_or_create("Other", hierarchy="Hierarchy")

        with pytest.raises(Region.NotUnique):
            platform.backend.regions.get_or_create("Other", hierarchy="Other Hierarchy")

    def test_list_region(self, platform: ixmp4.Platform) -> None:
        platform.backend.regions.create("Region 1", "Hierarchy")
        platform.backend.regions.create("Region 2", "Hierarchy")

        regions = platform.backend.regions.list()
        regions = sorted(regions, key=lambda x: x.id)
        assert regions[0].id == 1
        assert regions[0].name == "Region 1"
        assert regions[0].hierarchy == "Hierarchy"
        assert regions[1].id == 2
        assert regions[1].name == "Region 2"

    def test_tabulate_region(self, platform: ixmp4.Platform) -> None:
        platform.backend.regions.create("Region 1", "Hierarchy")
        platform.backend.regions.create("Region 2", "Hierarchy")

        true_regions = pd.DataFrame(
            [
                [1, "Region 1", "Hierarchy"],
                [2, "Region 2", "Hierarchy"],
            ],
            columns=["id", "name", "hierarchy"],
        )

        regions = platform.backend.regions.tabulate()
        assert_unordered_equality(
            regions.drop(columns=["created_at", "created_by"]), true_regions
        )

    def test_filter_region(self, platform: ixmp4.Platform) -> None:
        run1, run2 = self.filter.load_dataset(platform)

        res = platform.backend.regions.tabulate(
            iamc={
                "run": {"model": {"name": "Model 1"}},
                "unit": {"name": "Unit 1"},
            }
        )
        assert sorted(res["name"].tolist()) == ["Region 1", "Region 3"]

        run2.set_as_default()
        res = platform.backend.regions.tabulate(
            iamc={
                "variable": {"name__in": ["Variable 3", "Variable 5"]},
            }
        )
        assert sorted(res["name"].tolist()) == ["Region 2", "Region 3"]

        run2.unset_as_default()
        res = platform.backend.regions.tabulate(
            iamc={
                "variable": {"name__like": "Variable *"},
                "unit": {"name__in": ["Unit 1", "Unit 3"]},
                "run": {
                    "model": {"name__in": ["Model 1", "Model 2"]},
                    "default_only": True,
                },
            }
        )
        assert res["name"].tolist() == ["Region 1", "Region 3"]

        res = platform.backend.regions.tabulate(
            iamc={
                "variable": {"name__like": "Variable *"},
                "unit": {"name__in": ["Unit 1", "Unit 3"]},
                "run": {
                    "model": {"name__in": ["Model 1", "Model 2"]},
                    "default_only": False,
                },
            }
        )
        assert sorted(res["name"].tolist()) == [
            "Region 1",
            "Region 2",
            "Region 3",
            "Region 4",
        ]

        res = platform.backend.regions.tabulate(iamc=False)

        assert res["name"].tolist() == ["Region 5"]

        res = platform.backend.regions.tabulate()

        assert sorted(res["name"].tolist()) == [
            "Region 1",
            "Region 2",
            "Region 3",
            "Region 4",
            "Region 5",
        ]
