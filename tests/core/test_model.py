import pandas as pd
import pytest

from ixmp4 import Model

from ..utils import all_platforms, assert_unordered_equality


def create_testcase_models(test_mp):
    model = test_mp.models.create("Model")
    model2 = test_mp.models.create("Model 2")
    return model, model2


def df_from_list(models):
    return pd.DataFrame(
        [[m.id, m.name, m.created_at, m.created_by] for m in models],
        columns=["id", "name", "created_at", "created_by"],
    )


@all_platforms
class TestCoreModel:
    def test_retrieve_model(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        model1 = test_mp.models.create("Model")
        model2 = test_mp.models.get("Model")

        assert model1.id == model2.id

    def test_model_unqiue(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        test_mp.models.create("Model")

        with pytest.raises(Model.NotUnique):
            test_mp.models.create("Model")

    def test_list_model(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        models = create_testcase_models(test_mp)
        model, _ = models

        a = [m.id for m in models]
        b = [m.id for m in test_mp.models.list()]
        assert not (set(a) ^ set(b))

        a = [model.id]
        b = [m.id for m in test_mp.models.list(name="Model")]
        assert not (set(a) ^ set(b))

    def test_tabulate_model(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        models = create_testcase_models(test_mp)
        model, _ = models

        a = df_from_list(models)
        b = test_mp.models.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([model])
        b = test_mp.models.tabulate(name="Model")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        test_mp.models.create("Model")
        docs_model1 = test_mp.models.set_docs("Model", "Description of test Model")
        docs_model2 = test_mp.models.get_docs("Model")

        assert docs_model1 == docs_model2

        model2 = test_mp.models.create("Model2")
        assert model2.docs is None

        model2.docs = "Description of test Model2"

        assert test_mp.models.get_docs("Model2") == model2.docs

    def test_delete_docs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        model = test_mp.models.create("Model")
        model.docs = "Description of test Model"
        model.docs = None

        assert model.docs is None

        model.docs = "Second description of test Model"
        del model.docs

        assert model.docs is None

        model.docs = "Third description of test Model"
        test_mp.models.delete_docs("Model")

        assert model.docs is None
