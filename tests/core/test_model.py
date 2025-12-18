import datetime

import pytest

import ixmp4
from ixmp4 import Model
from tests import backends

platform = backends.get_platform_fixture(scope="class")


class TestModel:
    def test_create_model(
        self, platform: ixmp4.Platform, fake_time: datetime.datetime
    ) -> None:
        model1 = platform.models.create("Model 1")
        model2 = platform.models.create("Model 2")
        model3 = platform.models.create("Model 3")
        model4 = platform.models.create("Model 4")

        assert model1.id == 1
        assert model1.name == "Model 1"
        assert model1.created_at == fake_time.replace(tzinfo=None)
        assert model1.created_by == "@unknown"
        assert model1.docs is None
        assert str(model1) == "<Model 1 name='Model 1'>"

        assert model2.id == 2
        assert model3.id == 3
        assert model4.id == 4

    def test_tabulate_model(self, platform: ixmp4.Platform) -> None:
        ret_df = platform.models.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

    def test_list_model(self, platform: ixmp4.Platform) -> None:
        assert len(platform.models.list()) == 4

    def test_delete_model_via_func_obj(self, platform: ixmp4.Platform) -> None:
        model1 = platform.models.get_by_name("Model 1")
        platform.models.delete(model1)

    def test_delete_model_via_func_id(self, platform: ixmp4.Platform) -> None:
        platform.models.delete(2)

    def test_delete_model_via_func_name(self, platform: ixmp4.Platform) -> None:
        platform.models.delete("Model 3")

    def test_delete_model_via_obj(self, platform: ixmp4.Platform) -> None:
        model4 = platform.models.get_by_name("Model 4")
        model4.delete()

    def test_models_empty(self, platform: ixmp4.Platform) -> None:
        assert platform.models.tabulate().empty
        assert len(platform.models.list()) == 0


class TestModelUnique:
    def test_model_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.models.create("Model")

        with pytest.raises(Model.NotUnique):
            platform.models.create("Model")


class TestModelDocs:
    def test_create_docs_via_func(self, platform: ixmp4.Platform) -> None:
        model1 = platform.models.create("Model 1")

        model1_docs1 = platform.models.set_docs("Model 1", "Description of Model 1")
        model1_docs2 = platform.models.get_docs("Model 1")

        assert model1_docs1 == model1_docs2
        assert model1.docs == model1_docs1

    def test_create_docs_via_object(self, platform: ixmp4.Platform) -> None:
        model2 = platform.models.create("Model 2")
        model2.docs = "Description of Model 2"

        assert platform.models.get_docs("Model 2") == model2.docs

    def test_create_docs_via_setattr(self, platform: ixmp4.Platform) -> None:
        model3 = platform.models.create("Model 3")
        setattr(model3, "docs", "Description of Model 3")

        assert platform.models.get_docs("Model 3") == model3.docs

    def test_list_docs(self, platform: ixmp4.Platform) -> None:
        assert platform.models.list_docs() == [
            "Description of Model 1",
            "Description of Model 2",
            "Description of Model 3",
        ]

        assert platform.models.list_docs(id=3) == ["Description of Model 3"]

        assert platform.models.list_docs(id__in=[1]) == ["Description of Model 1"]

    def test_delete_docs_via_func(self, platform: ixmp4.Platform) -> None:
        model1 = platform.models.get_by_name("Model 1")
        platform.models.delete_docs("Model 1")
        model1 = platform.models.get_by_name("Model 1")
        assert model1.docs is None

    def test_delete_docs_set_none(self, platform: ixmp4.Platform) -> None:
        model2 = platform.models.get_by_name("Model 2")
        model2.docs = None
        model2 = platform.models.get_by_name("Model 2")
        assert model2.docs is None

    def test_delete_docs_del(self, platform: ixmp4.Platform) -> None:
        model3 = platform.models.get_by_name("Model 3")
        del model3.docs
        model3 = platform.models.get_by_name("Model 3")
        assert model3.docs is None

    def test_docs_empty(self, platform: ixmp4.Platform) -> None:
        assert len(platform.models.list_docs()) == 0
