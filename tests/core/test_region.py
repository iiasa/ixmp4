from collections.abc import Iterable

import pandas as pd
import pytest

import ixmp4
from ixmp4 import DataPoint
from ixmp4.core import Region

from ..fixtures import SmallIamcDataset
from ..utils import assert_unordered_equality


def create_testcase_regions(platform: ixmp4.Platform) -> tuple[Region, Region]:
    reg = platform.regions.create("Test", hierarchy="default")
    other = platform.regions.create("Test Other", hierarchy="other")
    return reg, other


def df_from_list(regions: Iterable[Region]) -> pd.DataFrame:
    return pd.DataFrame(
        [[r.id, r.name, r.hierarchy, r.created_at, r.created_by] for r in regions],
        columns=["id", "name", "hierarchy", "created_at", "created_by"],
    )


class TestCoreRegion:
    small = SmallIamcDataset

    def test_delete_region(self, platform: ixmp4.Platform) -> None:
        reg1 = platform.regions.create("Test 1", hierarchy="default")
        reg2 = platform.regions.create("Test 2", hierarchy="default")
        reg3 = platform.regions.create("Test 3", hierarchy="default")
        platform.regions.create("Test 4", hierarchy="default")

        assert reg1.id != reg2.id != reg3.id
        platform.regions.delete(reg1)
        platform.regions.delete(reg2.id)
        reg3.delete()
        platform.regions.delete("Test 4")

        assert platform.regions.tabulate().empty

        self.small.load_regions(platform)
        self.small.load_units(platform)

        run = platform.runs.create("Model", "Scenario")

        with run.transact("Add iamc data"):
            run.iamc.add(self.small.annual.copy(), type=DataPoint.Type.ANNUAL)

        with pytest.raises(Region.DeletionPrevented):
            platform.regions.delete("Region 1")

    def test_region_has_hierarchy(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(TypeError):
            # We are testing exactly this: raising with missing argument.
            platform.regions.create("Test Region")  # type: ignore[call-arg]

        reg1 = platform.regions.create("Test", hierarchy="default")
        reg2 = platform.regions.get("Test")

        assert reg1.id == reg2.id

    def test_get_region(self, platform: ixmp4.Platform) -> None:
        reg1 = platform.regions.create("Test", hierarchy="default")
        reg2 = platform.regions.get("Test")

        assert reg1.id == reg2.id

        with pytest.raises(Region.NotFound):
            platform.regions.get("Does not exist")

    def test_region_unique(self, platform: ixmp4.Platform) -> None:
        platform.regions.create("Test", hierarchy="default")

        with pytest.raises(Region.NotUnique):
            platform.regions.create("Test", hierarchy="other")

    def test_region_unknown(self, platform: ixmp4.Platform) -> None:
        self.small.load_regions(platform)
        self.small.load_units(platform)

        invalid_data = self.small.annual.copy()
        invalid_data["region"] = "invalid"

        run = platform.runs.create("Model", "Scenario")
        with pytest.raises(Region.NotFound):
            with run.transact("Add invalid data"):
                run.iamc.add(invalid_data, type=DataPoint.Type.ANNUAL)

    def test_list_region(self, platform: ixmp4.Platform) -> None:
        regions = create_testcase_regions(platform)
        reg, other = regions

        a = [r.id for r in regions]
        b = [r.id for r in platform.regions.list()]
        assert not (set(a) ^ set(b))

        a = [other.id]
        b = [r.id for r in platform.regions.list(hierarchy="other")]
        assert not (set(a) ^ set(b))

    def test_tabulate_region(self, platform: ixmp4.Platform) -> None:
        regions = create_testcase_regions(platform)
        _, other = regions

        a = df_from_list(regions)
        b = platform.regions.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([other])
        b = platform.regions.tabulate(hierarchy="other")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, platform: ixmp4.Platform) -> None:
        platform.regions.create("Test Region", "Test Hierarchy")
        docs_region1 = platform.regions.set_docs(
            "Test Region", "Description of test Region"
        )
        docs_region2 = platform.regions.get_docs("Test Region")

        assert docs_region1 == docs_region2

        region2 = platform.regions.create("Test Region 2", "Hierarchy")

        assert region2.docs is None

        region2.docs = "Description of test region 2"

        assert platform.regions.get_docs("Test Region 2") == region2.docs

    def test_delete_docs(self, platform: ixmp4.Platform) -> None:
        region = platform.regions.create("Test Region", "Hierarchy")
        region.docs = "Description of test region"
        region.docs = None

        assert region.docs is None

        region.docs = "Second description of test region"
        del region.docs

        assert region.docs is None

        # Mypy doesn't recognize del properly, it seems
        region.docs = "Third description of test region"  # type: ignore[unreachable]
        platform.regions.delete_docs("Test Region")

        assert region.docs is None
