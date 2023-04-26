import pytest
import pandas as pd

from ixmp4 import Unit

from ..utils import assert_unordered_equality, all_platforms, create_filter_test_data


@all_platforms
def test_create_get_unit(test_mp):
    unit1 = test_mp.backend.units.create("Unit")
    assert unit1.name == "Unit"

    unit2 = test_mp.backend.units.get("Unit")
    assert unit1.id == unit2.id


@all_platforms
def test_delete_unit(test_mp):
    unit1 = test_mp.backend.units.create("Unit")
    test_mp.backend.units.delete(unit1.id)
    assert test_mp.backend.units.tabulate().empty


@all_platforms
def test_get_or_create_unit(test_mp):
    unit1 = test_mp.backend.units.create("Unit")
    unit2 = test_mp.backend.units.get_or_create("Unit")
    assert unit1.id == unit2.id

    unit3 = test_mp.backend.units.get_or_create("Another Unit")
    assert unit3.name == "Another Unit"
    assert unit1.id != unit3.id


@all_platforms
def test_unit_unique(test_mp):
    test_mp.backend.units.create("Unit")

    with pytest.raises(Unit.NotUnique):
        test_mp.backend.units.create("Unit")


@all_platforms
def test_unit_not_found(test_mp):
    with pytest.raises(Unit.NotFound):
        test_mp.backend.units.get("Unit")


@all_platforms
def test_list_unit(test_mp):
    test_mp.backend.units.create("Unit 1")
    test_mp.backend.units.create("Unit 2")

    units = test_mp.backend.units.list()
    units = sorted(units, key=lambda x: x.id)

    assert units[0].id == 1
    assert units[0].name == "Unit 1"
    assert units[1].id == 2
    assert units[1].name == "Unit 2"


@all_platforms
def test_tabulate_unit(test_mp):
    test_mp.backend.units.create("Unit 1")
    test_mp.backend.units.create("Unit 2")

    true_units = pd.DataFrame(
        [
            [1, "Unit 1"],
            [2, "Unit 2"],
        ],
        columns=["id", "name"],
    )

    units = test_mp.backend.units.tabulate()
    assert_unordered_equality(
        units.drop(columns=["created_at", "created_by"]), true_units
    )


@all_platforms
def test_filter_unit(test_mp):
    run1, run2 = create_filter_test_data(test_mp)
    res = test_mp.backend.units.tabulate(
        iamc={
            "run": {"model": {"name": "Model 1"}},
            "variable": {"name": "Variable 1"},
        }
    )
    assert sorted(res["name"].tolist()) == ["Unit 1", "Unit 2"]

    run2.set_as_default()
    res = test_mp.backend.units.tabulate(
        iamc={
            "run": {"model": {"name": "Model 1"}},
            "variable": {"name": "Variable 1"},
        }
    )
    assert sorted(res["name"].tolist()) == ["Unit 3", "Unit 4"]

    run1.set_as_default()
    res = test_mp.backend.units.tabulate(
        iamc={
            "variable": {"name": "Variable 1"},
            "region": {"name__in": ["Region 5", "Region 6"]},
            "run": {"model": {"name": "Model 1"}, "default_only": True},
        }
    )
    assert res["name"].tolist() == []

    res = test_mp.backend.units.tabulate(
        iamc={
            "variable": {"name": "Variable 1"},
            "region": {"name__in": ["Region 5", "Region 6"]},
            "run": {"model": {"name": "Model 1"}, "default_only": False},
        }
    )
    assert sorted(res["name"].tolist()) == ["Unit 4"]

    res = test_mp.backend.units.tabulate(iamc=False)

    assert res["name"].tolist() == ["Unit 5"]

    res = test_mp.backend.units.tabulate()

    assert sorted(res["name"].tolist()) == [
        "Unit 1",
        "Unit 2",
        "Unit 3",
        "Unit 4",
        "Unit 5",
    ]
