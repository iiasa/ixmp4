import datetime

import pytest

import ixmp4
from ixmp4 import Unit
from tests import backends

platform = backends.get_platform_fixture(scope="class")


class TestUnit:
    def test_create_unit(
        self, platform: ixmp4.Platform, fake_time: datetime.datetime
    ) -> None:
        unit1 = platform.units.create("Unit 1")
        unit2 = platform.units.create("Unit 2")
        unit3 = platform.units.create("Unit 3")
        unit4 = platform.units.create("Unit 4")

        assert unit1.id == 1
        assert unit1.name == "Unit 1"
        assert unit1.created_at == fake_time.replace(tzinfo=None)
        assert unit1.created_by == "@unknown"
        assert unit1.docs is None
        assert str(unit1) == "<Unit 1 name=Unit 1>"

        assert unit2.id == 2
        assert unit3.id == 3
        assert unit4.id == 4

    def test_tabulate_unit(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.units.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

    def test_list_unit(self, platform: ixmp4.Platform) -> None:
        assert len(platform.units.list()) == 4

    def test_delete_unit_via_func_obj(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.get_by_name("Unit 1")
        platform.units.delete(unit1)

    def test_delete_unit_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.units.delete(2)

    def test_delete_unit_via_func_name(self, platform: ixmp4.Platform) -> None:
        platform.units.delete("Unit 3")

    def test_delete_unit_via_obj(self, platform: ixmp4.Platform) -> None:
        unit4 = platform.units.get_by_name("Unit 4")
        unit4.delete()

    def test_units_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.units.tabulate().empty
        assert len(platform.units.list()) == 0


class TestUnitUnique:
    def test_unit_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.units.create("Unit")

        with pytest.raises(Unit.NotUnique):
            platform.units.create("Unit")


class TestUnitNames:
    def test_unit_dimensionless(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.create("")
        unit2 = platform.units.get_by_name("")

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


class TestUnitDocs:
    def test_create_docs_via_func(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.create("Unit 1")

        unit1_docs1 = platform.units.set_docs("Unit 1", "Description of Unit 1")
        unit1_docs2 = platform.units.get_docs("Unit 1")

        assert unit1_docs1 == unit1_docs2
        assert unit1.docs == unit1_docs1

    def test_create_docs_via_object(self, platform: ixmp4.Platform) -> None:
        unit2 = platform.units.create("Unit 2")
        unit2.docs = "Description of Unit 2"

        assert platform.units.get_docs("Unit 2") == unit2.docs

    def test_create_docs_via_setattr(self, platform: ixmp4.Platform) -> None:
        unit3 = platform.units.create("Unit 3")
        setattr(unit3, "docs", "Description of Unit 3")

        assert platform.units.get_docs("Unit 3") == unit3.docs

    def test_list_docs(self, platform: ixmp4.Platform) -> None:
        assert platform.units.list_docs() == [
            "Description of Unit 1",
            "Description of Unit 2",
            "Description of Unit 3",
        ]

        assert platform.units.list_docs(id=3) == ["Description of Unit 3"]

        assert platform.units.list_docs(id__in=[1]) == ["Description of Unit 1"]

    def test_delete_docs_via_func(self, platform: ixmp4.Platform) -> None:
        unit1 = platform.units.get_by_name("Unit 1")
        platform.units.delete_docs("Unit 1")
        unit1 = platform.units.get_by_name("Unit 1")
        assert unit1.docs is None

    def test_delete_docs_set_none(self, platform: ixmp4.Platform) -> None:
        unit2 = platform.units.get_by_name("Unit 2")
        unit2.docs = None
        unit2 = platform.units.get_by_name("Unit 2")
        assert unit2.docs is None

    def test_delete_docs_del(self, platform: ixmp4.Platform) -> None:
        unit3 = platform.units.get_by_name("Unit 3")
        del unit3.docs
        unit3 = platform.units.get_by_name("Unit 3")
        assert unit3.docs is None

    def test_docs_empty(self, platform: ixmp4.Platform) -> None:
        assert len(platform.units.list_docs()) == 0
