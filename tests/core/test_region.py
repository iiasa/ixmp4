import datetime

import pytest

import ixmp4
from ixmp4 import Region
from tests import backends

platform = backends.get_platform_fixture(scope="class")


class TestRegion:
    def test_create_region(
        self, platform: ixmp4.Platform, fake_time: datetime.datetime
    ) -> None:
        region1 = platform.regions.create("Region 1", hierarchy="default")
        region2 = platform.regions.create("Region 2", hierarchy="default")
        region3 = platform.regions.create("Region 3", hierarchy="default")
        region4 = platform.regions.create("Region 4", hierarchy="default")

        assert region1.id == 1
        assert region1.name == "Region 1"
        assert region1.hierarchy == "default"
        assert region1.created_at == fake_time.replace(tzinfo=None)
        assert region1.created_by == "@unknown"
        assert region1.docs is None
        assert str(region1) == "<Region 1 name='Region 1' hierarchy='default'>"

        assert region2.id == 2
        assert region3.id == 3
        assert region4.id == 4

    def test_tabulate_region(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.regions.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "hierarchy" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

    def test_list_region(self, platform: ixmp4.Platform) -> None:
        assert len(platform.regions.list()) == 4

    def test_delete_region_via_func_obj(self, platform: ixmp4.Platform) -> None:
        region1 = platform.regions.get_by_name("Region 1")
        platform.regions.delete(region1)

    def test_delete_region_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.regions.delete(2)

    def test_delete_region_via_func_name(self, platform: ixmp4.Platform) -> None:
        platform.regions.delete("Region 3")

    def test_delete_region_via_obj(self, platform: ixmp4.Platform) -> None:
        region4 = platform.regions.get_by_name("Region 4")
        region4.delete()

    def test_regions_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.regions.tabulate().empty
        assert len(platform.regions.list()) == 0


class TestRegionUnique:
    def test_region_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.regions.create("Test", hierarchy="default")

        with pytest.raises(Region.NotUnique):
            platform.regions.create("Test", hierarchy="default")

        with pytest.raises(Region.NotUnique):
            platform.regions.create("Test", hierarchy="other")


class TestRegionDocs:
    def test_create_docs_via_func(self, platform: ixmp4.Platform) -> None:
        region1 = platform.regions.create("Region 1", hierarchy="default")

        region1_docs1 = platform.regions.set_docs("Region 1", "Description of Region 1")
        region1_docs2 = platform.regions.get_docs("Region 1")

        assert region1_docs1 == region1_docs2
        assert region1.docs == region1_docs1

    def test_create_docs_via_object(self, platform: ixmp4.Platform) -> None:
        region2 = platform.regions.create("Region 2", hierarchy="default")
        region2.docs = "Description of Region 2"

        assert platform.regions.get_docs("Region 2") == region2.docs

    def test_create_docs_via_setattr(self, platform: ixmp4.Platform) -> None:
        region3 = platform.regions.create("Region 3", hierarchy="default")
        setattr(region3, "docs", "Description of Region 3")

        assert platform.regions.get_docs("Region 3") == region3.docs

    def test_list_docs(self, platform: ixmp4.Platform) -> None:
        assert platform.regions.list_docs() == [
            "Description of Region 1",
            "Description of Region 2",
            "Description of Region 3",
        ]

        assert platform.regions.list_docs(id=3) == ["Description of Region 3"]

        assert platform.regions.list_docs(id__in=[1]) == ["Description of Region 1"]

    def test_delete_docs_via_func(self, platform: ixmp4.Platform) -> None:
        region1 = platform.regions.get_by_name("Region 1")
        platform.regions.delete_docs("Region 1")
        region1 = platform.regions.get_by_name("Region 1")
        assert region1.docs is None

    def test_delete_docs_set_none(self, platform: ixmp4.Platform) -> None:
        region2 = platform.regions.get_by_name("Region 2")
        region2.docs = None
        region2 = platform.regions.get_by_name("Region 2")
        assert region2.docs is None

    def test_delete_docs_del(self, platform: ixmp4.Platform) -> None:
        region3 = platform.regions.get_by_name("Region 3")
        del region3.docs
        region3 = platform.regions.get_by_name("Region 3")
        assert region3.docs is None

    def test_docs_empty(self, platform: ixmp4.Platform) -> None:
        assert len(platform.regions.list_docs()) == 0
