from collections.abc import Iterable

import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Unit

from ..fixtures import SmallIamcDataset
from ..utils import assert_unordered_equality


def create_testcase_units(platform: ixmp4.Platform) -> tuple[Unit, Unit]:
    unit = platform.units.create("Test")
    unit2 = platform.units.create("Test 2")
    return unit, unit2


def df_from_list(units: Iterable[Unit]) -> pd.DataFrame:
    return pd.DataFrame(
        [[u.id, u.name, u.created_at, u.created_by] for u in units],
        columns=["id", "name", "created_at", "created_by"],
    )


class TestCoreUnit:
    small = SmallIamcDataset()

    def test_delete_unit(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.create("Test 1")
        unit2 = platform.units.create("Test 2")
        unit3 = platform.units.create("Test 3")
        platform.units.create("Test 4")

        assert unit1.id != unit2.id != unit3.id
        platform.units.delete(unit1)
        platform.units.delete(unit2.id)
        unit3.delete()
        platform.units.delete("Test 4")

        assert platform.units.tabulate().empty

        self.small.load_regions(platform)
        self.small.load_units(platform)

        run = platform.runs.create("Model", "Scenario")
        with run.transact("Add iamc data"):
            run.iamc.add(self.small.annual, type=ixmp4.DataPoint.Type.ANNUAL)

        with pytest.raises(Unit.DeletionPrevented):
            platform.units.delete("Unit 1")

    def test_retrieve_unit(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.create("Test")
        unit2 = platform.units.get("Test")

        assert unit1.id == unit2.id

    def test_unit_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.units.create("Test")

        with pytest.raises(Unit.NotUnique):
            platform.units.create("Test")

    def test_unit_dimensionless(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.create("")
        unit2 = platform.units.get("")

        assert unit1.id == unit2.id

        assert "" in platform.units.tabulate().values
        assert "" in [unit.name for unit in platform.units.list()]

    def test_unit_illegal_names(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(ValueError, match="Unit name 'dimensionless' is reserved,"):
            platform.units.create("dimensionless")

        with pytest.raises(
            ValueError, match="Using a space-only unit name is not allowed"
        ):
            platform.units.create("   ")

    def test_unit_unknown(self, platform: ixmp4.Platform) -> None:
        self.small.load_regions(platform)
        self.small.load_units(platform)

        invalid_data = self.small.annual.copy()
        invalid_data["unit"] = "foo"

        run = platform.runs.create("Model", "Scenario")
        with pytest.raises(Unit.NotFound):
            with run.transact("Add invalid data"):
                run.iamc.add(invalid_data, type=ixmp4.DataPoint.Type.ANNUAL)

    def test_list_unit(self, platform: ixmp4.Platform) -> None:
        units = create_testcase_units(platform)
        unit, _ = units

        a = [u.id for u in units]
        b = [u.id for u in platform.units.list()]
        assert not (set(a) ^ set(b))

        a = [unit.id]
        b = [u.id for u in platform.units.list(name="Test")]
        assert not (set(a) ^ set(b))

    def test_tabulate_unit(self, platform: ixmp4.Platform) -> None:
        units = create_testcase_units(platform)
        unit, _ = units

        a = df_from_list(units)
        b = platform.units.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([unit])
        b = platform.units.tabulate(name="Test")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, platform: ixmp4.Platform) -> None:
        platform.units.create("Unit")
        docs_unit1 = platform.units.set_docs("Unit", "Description of test Unit")
        docs_unit2 = platform.units.get_docs("Unit")

        assert docs_unit1 == docs_unit2

        unit2 = platform.units.create("Unit2")

        assert unit2.docs is None

        unit2.docs = "Description of test Unit2"

        assert platform.units.get_docs("Unit2") == unit2.docs

    def test_delete_docs(self, platform: ixmp4.Platform) -> None:
        unit = platform.units.create("Unit")
        unit.docs = "Description of test Unit"
        unit.docs = None

        assert unit.docs is None

        unit.docs = "Second description of test Unit"
        del unit.docs

        assert unit.docs is None

        # Mypy doesn't recognize del properly, it seems
        unit.docs = "Third description of test Unit"  # type: ignore[unreachable]
        platform.units.delete_docs("Unit")

        assert unit.docs is None

    def test_list_docs(self, platform: ixmp4.Platform) -> None:
        unit_1 = platform.units.create("Unit 1")
        unit_1.docs = "Description of Unit 1"
        unit_2 = platform.units.create("Unit 2")
        unit_2.docs = "Description of Unit 2"
        unit_3 = platform.units.create("Unit 3")
        unit_3.docs = "Description of Unit 3"

        assert platform.units.list_docs() == [unit_1.docs, unit_2.docs, unit_3.docs]

        assert platform.units.list_docs(id=unit_2.id) == [unit_2.docs]

        assert platform.units.list_docs(id__in=[unit_1.id, unit_3.id]) == [
            unit_1.docs,
            unit_3.docs,
        ]
