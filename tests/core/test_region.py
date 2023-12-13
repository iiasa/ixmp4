import pandas as pd
import pytest

from ixmp4 import DataPoint, Region

from ..utils import add_regions, add_units, all_platforms, assert_unordered_equality


def create_testcase_regions(test_mp):
    reg = test_mp.regions.create("Test", hierarchy="default")
    other = test_mp.regions.create("Test Other", hierarchy="other")
    return reg, other


def df_from_list(regions):
    return pd.DataFrame(
        [[r.id, r.name, r.hierarchy, r.created_at, r.created_by] for r in regions],
        columns=["id", "name", "hierarchy", "created_at", "created_by"],
    )


@all_platforms
class TestCoreRegion:
    def test_delete_region(self, test_mp, test_data_annual):
        reg1 = test_mp.regions.create("Test 1", hierarchy="default")
        reg2 = test_mp.regions.create("Test 2", hierarchy="default")
        reg3 = test_mp.regions.create("Test 3", hierarchy="default")
        test_mp.regions.create("Test 4", hierarchy="default")

        assert reg1.id != reg2.id != reg3.id
        test_mp.regions.delete(reg1)
        test_mp.regions.delete(reg2.id)
        reg3.delete()
        test_mp.regions.delete("Test 4")

        assert test_mp.regions.tabulate().empty

        add_regions(test_mp, test_data_annual["region"].unique())
        add_units(test_mp, test_data_annual["unit"].unique())

        run = test_mp.runs.create("Model", "Scenario")
        run.iamc.add(test_data_annual, type=DataPoint.Type.ANNUAL)

        with pytest.raises(Region.DeletionPrevented):
            test_mp.regions.delete("World")

    def test_region_has_hierarchy(self, test_mp):
        with pytest.raises(TypeError):
            test_mp.regions.create("Test")

        reg1 = test_mp.regions.create("Test", hierarchy="default")
        reg2 = test_mp.regions.get("Test")

        assert reg1.id == reg2.id

    def test_get_region(self, test_mp):
        reg1 = test_mp.regions.create("Test", hierarchy="default")
        reg2 = test_mp.regions.get("Test")

        assert reg1.id == reg2.id

        with pytest.raises(Region.NotFound):
            test_mp.regions.get("Does not exist")

    def test_region_unique(self, test_mp):
        test_mp.regions.create("Test", hierarchy="default")

        with pytest.raises(Region.NotUnique):
            test_mp.regions.create("Test", hierarchy="other")

    def test_region_unknown(self, test_mp, test_data_annual):
        add_regions(test_mp, test_data_annual["region"].unique())
        add_units(test_mp, test_data_annual["unit"].unique())

        test_data_annual["region"] = "foo"

        run = test_mp.runs.create("Model", "Scenario")
        with pytest.raises(Region.NotFound):
            run.iamc.add(test_data_annual, type=DataPoint.Type.ANNUAL)

    def test_list_region(self, test_mp):
        regions = create_testcase_regions(test_mp)
        reg, other = regions

        a = [r.id for r in regions]
        b = [r.id for r in test_mp.regions.list()]
        assert not (set(a) ^ set(b))

        a = [other.id]
        b = [r.id for r in test_mp.regions.list(hierarchy="other")]
        assert not (set(a) ^ set(b))

    def test_tabulate_region(self, test_mp):
        regions = create_testcase_regions(test_mp)
        _, other = regions

        a = df_from_list(regions)
        b = test_mp.regions.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([other])
        b = test_mp.regions.tabulate(hierarchy="other")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, test_mp):
        test_mp.regions.create("Region", "Hierarchy")
        docs_region1 = test_mp.regions.set_docs("Region", "Description of test Region")
        docs_region2 = test_mp.regions.get_docs("Region")

        assert docs_region1 == docs_region2

        region2 = test_mp.regions.create("Region2", "Hierarchy")

        assert region2.docs is None

        region2.docs = "Description of test region2"

        assert test_mp.regions.get_docs("Region2") == region2.docs

    def test_delete_docs(self, test_mp):
        region = test_mp.regions.create("Region", "Hierarchy")
        region.docs = "Description of test region"
        region.docs = None

        assert region.docs is None

        region.docs = "Second description of test region"
        del region.docs

        assert region.docs is None

        region.docs = "Third description of test region"
        test_mp.regions.delete_docs("Region")

        assert region.docs is None
