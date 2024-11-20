from collections.abc import Iterable

import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Model

from ..utils import assert_unordered_equality


def create_testcase_models(test_mp: ixmp4.Platform) -> tuple[Model, Model]:
    model = test_mp.models.create("Model")
    model2 = test_mp.models.create("Model 2")
    return model, model2


def df_from_list(models: Iterable[Model]) -> pd.DataFrame:
    return pd.DataFrame(
        [[m.id, m.name, m.created_at, m.created_by] for m in models],
        columns=["id", "name", "created_at", "created_by"],
    )


class TestCoreModel:
    def test_retrieve_model(self, platform: ixmp4.Platform) -> None:
        model1 = platform.models.create("Model")
        model2 = platform.models.get("Model")

        assert model1.id == model2.id

    def test_model_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.models.create("Model")

        with pytest.raises(Model.NotUnique):
            platform.models.create("Model")

    def test_list_model(self, platform: ixmp4.Platform) -> None:
        models = create_testcase_models(platform)
        model, _ = models

        a = [m.id for m in models]
        b = [m.id for m in platform.models.list()]
        assert not (set(a) ^ set(b))

        a = [model.id]
        b = [m.id for m in platform.models.list(name="Model")]
        assert not (set(a) ^ set(b))

    def test_tabulate_model(self, platform: ixmp4.Platform) -> None:
        models = create_testcase_models(platform)
        model, _ = models

        a = df_from_list(models)
        b = platform.models.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([model])
        b = platform.models.tabulate(name="Model")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, platform: ixmp4.Platform) -> None:
        platform.models.create("Model")
        docs_model1 = platform.models.set_docs("Model", "Description of test Model")
        docs_model2 = platform.models.get_docs("Model")

        assert docs_model1 == docs_model2

        model2 = platform.models.create("Model2")
        assert model2.docs is None

        model2.docs = "Description of test Model2"

        assert platform.models.get_docs("Model2") == model2.docs

    def test_delete_docs(self, platform: ixmp4.Platform) -> None:
        model = platform.models.create("Model")
        model.docs = "Description of test Model"
        model.docs = None

        assert model.docs is None

        model.docs = "Second description of test Model"
        del model.docs

        assert model.docs is None

        # Mypy doesn't recognize del properly, it seems
        model.docs = "Third description of test Model"  # type: ignore[unreachable]
        platform.models.delete_docs("Model")

        assert model.docs is None
