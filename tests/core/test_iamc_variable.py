import datetime

import pytest

import ixmp4
from ixmp4 import iamc
from tests import backends

platform = backends.get_platform_fixture(scope="class")


class TestVariable:
    def test_create_variable(
        self, platform: ixmp4.Platform, fake_time: datetime.datetime
    ) -> None:
        variable1 = platform.iamc.variables.create("Variable 1")
        variable2 = platform.iamc.variables.create("Variable 2")
        variable3 = platform.iamc.variables.create("Variable 3")
        variable4 = platform.iamc.variables.create("Variable 4")

        assert variable1.id == 1
        assert variable1.name == "Variable 1"
        assert variable1.created_at == fake_time.replace(tzinfo=None)
        assert variable1.created_by == "@unknown"
        assert variable1.docs is None
        assert str(variable1) == "<Variable 1 name='Variable 1'>"

        assert variable2.id == 2
        assert variable3.id == 3
        assert variable4.id == 4

    def test_tabulate_variable(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.iamc.variables.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

    def test_list_variable(self, platform: ixmp4.Platform) -> None:
        assert len(platform.iamc.variables.list()) == 4

    def test_delete_variable_via_func_obj(self, platform: ixmp4.Platform) -> None:
        variable1 = platform.iamc.variables.get_by_name("Variable 1")
        platform.iamc.variables.delete(variable1)

    def test_delete_variable_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.iamc.variables.delete(2)

    def test_delete_variable_via_func_name(self, platform: ixmp4.Platform) -> None:
        platform.iamc.variables.delete("Variable 3")

    def test_delete_variable_via_obj(self, platform: ixmp4.Platform) -> None:
        variable4 = platform.iamc.variables.get_by_name("Variable 4")
        variable4.delete()

    def test_variables_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.iamc.variables.tabulate().empty
        assert len(platform.iamc.variables.list()) == 0


class TestVariableUnique:
    def test_variable_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.iamc.variables.create("Variable")

        with pytest.raises(iamc.Variable.NotUnique):
            platform.iamc.variables.create("Variable")


class TestVariableDocs:
    def test_create_docs_via_func(self, platform: ixmp4.Platform) -> None:
        variable1 = platform.iamc.variables.create("Variable 1")

        variable1_docs1 = platform.iamc.variables.set_docs(
            "Variable 1", "Description of Variable 1"
        )
        variable1_docs2 = platform.iamc.variables.get_docs("Variable 1")

        assert variable1_docs1 == variable1_docs2
        assert variable1.docs == variable1_docs1

    def test_create_docs_via_object(self, platform: ixmp4.Platform) -> None:
        variable2 = platform.iamc.variables.create("Variable 2")
        variable2.docs = "Description of Variable 2"

        assert platform.iamc.variables.get_docs("Variable 2") == variable2.docs

    def test_create_docs_via_setattr(self, platform: ixmp4.Platform) -> None:
        variable3 = platform.iamc.variables.create("Variable 3")
        setattr(variable3, "docs", "Description of Variable 3")

        assert platform.iamc.variables.get_docs("Variable 3") == variable3.docs

    def test_list_docs(self, platform: ixmp4.Platform) -> None:
        assert platform.iamc.variables.list_docs() == [
            "Description of Variable 1",
            "Description of Variable 2",
            "Description of Variable 3",
        ]

        assert platform.iamc.variables.list_docs(id=3) == ["Description of Variable 3"]

        assert platform.iamc.variables.list_docs(id__in=[1]) == [
            "Description of Variable 1"
        ]

    def test_delete_docs_via_func(self, platform: ixmp4.Platform) -> None:
        variable1 = platform.iamc.variables.get_by_name("Variable 1")
        platform.iamc.variables.delete_docs("Variable 1")
        variable1 = platform.iamc.variables.get_by_name("Variable 1")
        assert variable1.docs is None

    def test_delete_docs_set_none(self, platform: ixmp4.Platform) -> None:
        variable2 = platform.iamc.variables.get_by_name("Variable 2")
        variable2.docs = None
        variable2 = platform.iamc.variables.get_by_name("Variable 2")
        assert variable2.docs is None

    def test_delete_docs_del(self, platform: ixmp4.Platform) -> None:
        variable3 = platform.iamc.variables.get_by_name("Variable 3")
        del variable3.docs
        variable3 = platform.iamc.variables.get_by_name("Variable 3")
        assert variable3.docs is None

    def test_docs_empty(self, platform: ixmp4.Platform) -> None:
        assert len(platform.iamc.variables.list_docs()) == 0
