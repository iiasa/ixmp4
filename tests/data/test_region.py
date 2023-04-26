import pytest
import pandas as pd

from ixmp4 import Region

from ..utils import assert_unordered_equality, all_platforms, create_filter_test_data


@all_platforms
def test_create_region(test_mp):
    region1 = test_mp.backend.regions.create("Region", "Hierarchy")
    assert region1.name == "Region"
    assert region1.hierarchy == "Hierarchy"
    assert region1.created_at is not None
    assert region1.created_by == "@unknown"


@all_platforms
def test_delete_region(test_mp):
    region1 = test_mp.backend.regions.create("Region", "Hierarchy")
    test_mp.backend.regions.delete(region1.id)
    assert test_mp.backend.regions.tabulate().empty


@all_platforms
def test_region_unique(test_mp):
    test_mp.backend.regions.create("Region", "Hierarchy")

    with pytest.raises(Region.NotUnique):
        test_mp.regions.create("Region", "Hierarchy")

    with pytest.raises(Region.NotUnique):
        test_mp.regions.create("Region", "Another Hierarchy")


@all_platforms
def test_get_region(test_mp):
    region1 = test_mp.backend.regions.create("Region", "Hierarchy")
    region2 = test_mp.backend.regions.get("Region")
    assert region1 == region2


@all_platforms
def test_region_not_found(test_mp):
    with pytest.raises(Region.NotFound):
        test_mp.regions.get("Region")


@all_platforms
def test_get_or_create_region(test_mp):
    region1 = test_mp.backend.regions.create("Region", "Hierarchy")
    region2 = test_mp.backend.regions.get_or_create("Region")
    assert region1.id == region2.id

    test_mp.backend.regions.get_or_create("Other", hierarchy="Hierarchy")

    with pytest.raises(Region.NotUnique):
        test_mp.backend.regions.get_or_create("Other", hierarchy="Other Hierarchy")


@all_platforms
def test_list_region(test_mp):
    region1 = test_mp.backend.regions.create("Region 1", "Hierarchy")
    test_mp.backend.regions.create("Region 2", "Hierarchy")

    regions = test_mp.backend.regions.list()
    regions = sorted(regions, key=lambda x: x.id)
    assert regions[0].id == 1
    assert regions[0].name == "Region 1"
    assert regions[0].hierarchy == "Hierarchy"
    assert regions[1].id == 2
    assert regions[1].name == "Region 2"


@all_platforms
def test_tabulate_region(test_mp):
    region1 = test_mp.backend.regions.create("Region 1", "Hierarchy")
    test_mp.backend.regions.create("Region 2", "Hierarchy")

    true_regions = pd.DataFrame(
        [
            [1, "Region 1", "Hierarchy"],
            [2, "Region 2", "Hierarchy"],
        ],
        columns=["id", "name", "hierarchy"],
    )

    regions = test_mp.backend.regions.tabulate()
    assert_unordered_equality(
        regions.drop(columns=["created_at", "created_by"]), true_regions
    )


@all_platforms
def test_filter_region(test_mp):
    run1, run2 = create_filter_test_data(test_mp)

    res = test_mp.backend.regions.tabulate(
        iamc={
            "run": {"model": {"name": "Model 1"}},
            "variable": {"name": "Variable 1"},
        }
    )
    assert sorted(res["name"].tolist()) == ["Region 1", "Region 3"]

    run2.set_as_default()
    res = test_mp.backend.regions.tabulate(
        iamc={
            "run": {"model": {"name": "Model 1"}},
            "variable": {"name": "Variable 1"},
        }
    )
    assert sorted(res["name"].tolist()) == ["Region 3", "Region 5"]

    run1.set_as_default()
    res = test_mp.backend.regions.tabulate(
        iamc={
            "variable": {"name": "Variable 1"},
            "unit": {"name__in": ["Unit 3", "Unit 4"]},
            "run": {"model": {"name": "Model 1"}, "default_only": True},
        }
    )
    assert res["name"].tolist() == []

    res = test_mp.backend.regions.tabulate(
        iamc={
            "variable": {"name": "Variable 1"},
            "unit": {"name__in": ["Unit 3", "Unit 4"]},
            "run": {"model": {"name": "Model 1"}, "default_only": False},
        }
    )
    assert sorted(res["name"].tolist()) == ["Region 3", "Region 5"]

    res = test_mp.backend.regions.tabulate(iamc=False)

    assert res["name"].tolist() == ["Region 7"]

    res = test_mp.backend.regions.tabulate()

    assert sorted(res["name"].tolist()) == [
        "Region 1",
        "Region 2",
        "Region 3",
        "Region 4",
        "Region 5",
        "Region 6",
        "Region 7",
    ]
