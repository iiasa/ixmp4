import pytest
import pandas as pd

from ixmp4 import DataPoint, Unit

from ..utils import add_regions, add_units, assert_unordered_equality, all_platforms


@all_platforms
def test_delete_unit(test_mp, test_data_annual):
    unit1 = test_mp.units.create("Test 1")
    unit2 = test_mp.units.create("Test 2")
    unit3 = test_mp.units.create("Test 3")
    test_mp.units.create("Test 4")

    assert unit1.id != unit2.id != unit3.id
    test_mp.units.delete(unit1)
    test_mp.units.delete(unit2.id)
    unit3.delete()
    test_mp.units.delete("Test 4")

    assert test_mp.units.tabulate().empty

    add_regions(test_mp, test_data_annual["region"].unique())
    add_units(test_mp, test_data_annual["unit"].unique())

    run = test_mp.Run("Model", "Scenario", version="new")
    run.iamc.add(test_data_annual, type=DataPoint.Type.ANNUAL)

    with pytest.raises(Unit.DeletionPrevented):
        test_mp.units.delete("EJ/yr")


@all_platforms
def test_retrieve_unit(test_mp):
    unit1 = test_mp.units.create("Test")
    unit2 = test_mp.units.get("Test")

    assert unit1.id == unit2.id


@all_platforms
def test_unit_unqiue(test_mp):
    test_mp.units.create("Test")

    with pytest.raises(Unit.NotUnique):
        test_mp.units.create("Test")


@all_platforms
def test_unit_dimensionless(test_mp):
    unit1 = test_mp.units.create("")
    unit2 = test_mp.units.get("")

    assert unit1.id == unit2.id


@all_platforms
def test_unit_illegal_names(test_mp):
    with pytest.raises(ValueError, match="Unit name 'dimensionless' is reserved,"):
        test_mp.units.create("dimensionless")

    with pytest.raises(ValueError, match="Using a space-only unit name is not allowed"):
        test_mp.units.create("   ")


@all_platforms
def test_unit_unknown(test_mp, test_data_annual):
    add_regions(test_mp, test_data_annual["region"].unique())
    add_units(test_mp, test_data_annual["unit"].unique())

    test_data_annual["unit"] = "foo"

    run = test_mp.Run("Model", "Scenario", version="new")
    with pytest.raises(Unit.NotFound):
        run.iamc.add(test_data_annual, type=DataPoint.Type.ANNUAL)


def create_testcase_units(test_mp):
    unit = test_mp.units.create("Test")
    unit2 = test_mp.units.create("Test 2")
    return unit, unit2


@all_platforms
def test_list_unit(test_mp):
    units = create_testcase_units(test_mp)
    unit, _ = units

    a = [u.id for u in units]
    b = [u.id for u in test_mp.units.list()]
    assert not (set(a) ^ set(b))

    a = [unit.id]
    b = [u.id for u in test_mp.units.list(name="Test")]
    assert not (set(a) ^ set(b))


# TODO: tabulate does not include docs -> should it?
def df_from_list(units):
    return pd.DataFrame(
        [[u.id, u.name, u.created_at, u.created_by] for u in units],
        columns=["id", "name", "created_at", "created_by"],
    )


@all_platforms
def test_tabulate_unit(test_mp):
    units = create_testcase_units(test_mp)
    unit, _ = units

    a = df_from_list(units)
    b = test_mp.units.tabulate()
    assert_unordered_equality(a, b, check_dtype=False)

    a = df_from_list([unit])
    b = test_mp.units.tabulate(name="Test")
    assert_unordered_equality(a, b, check_dtype=False)


@all_platforms
def test_retrieve_docs(test_mp):
    test_mp.units.create("Unit")
    docs_unit1 = test_mp.units.set_docs("Unit", "Description of test Unit")
    docs_unit2 = test_mp.units.get_docs("Unit")

    assert docs_unit1 == docs_unit2

    unit2 = test_mp.units.create("Unit2")

    assert unit2.docs is None

    unit2.docs = "Description of test Unit2"

    assert test_mp.units.get_docs("Unit2") == unit2.docs


@all_platforms
def test_delete_docs(test_mp):
    unit = test_mp.units.create("Unit")
    unit.docs = "Description of test Unit"
    unit.docs = None

    assert unit.docs is None

    unit.docs = "Second description of test Unit"
    del unit.docs

    assert unit.docs is None

    unit.docs = "Third description of test Unit"
    test_mp.units.delete_docs("Unit")

    assert unit.docs is None
