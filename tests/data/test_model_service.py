import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.model.repositories import ModelNotFound, ModelNotUnique
from ixmp4.data.model.service import ModelService
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class ModelServiceTest(ServiceTest[ModelService]):
    service_class = ModelService


class TestModelCreate(ModelServiceTest):
    def test_model_create(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        model = service.create("Model")
        assert model.name == "Model"
        assert model.created_at == fake_time.replace(tzinfo=None)
        assert model.created_by == "@unknown"

    def test_model_create_versioning(
        self, versioning_service: ModelService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Model",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestModelDeleteById(ModelServiceTest):
    def test_model_delete_by_id(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        model = service.create("Model")
        service.delete_by_id(model.id)
        assert service.tabulate().empty

    def test_model_delete_by_id_versioning(
        self, versioning_service: ModelService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Model",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    2,
                    0,
                ],
                [
                    1,
                    "Model",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    2,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )


class TestModelUnique(ModelServiceTest):
    def test_model_unique(self, service: ModelService) -> None:
        service.create("Model")

        with pytest.raises(ModelNotUnique):
            service.create("Model")


class TestModelGetByName(ModelServiceTest):
    def test_model_get_by_name(self, service: ModelService) -> None:
        model1 = service.create("Model")
        model2 = service.get_by_name("Model")
        assert model1 == model2


class TestModelGetById(ModelServiceTest):
    def test_model_get_by_id(self, service: ModelService) -> None:
        model1 = service.create("Model")
        model2 = service.get_by_id(1)
        assert model1 == model2


class TestModelNotFound(ModelServiceTest):
    def test_model_not_found(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_name("Model")

        with pytest.raises(ModelNotFound):
            service.get_by_id(1)


class TestModelList(ModelServiceTest):
    def test_model_list(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        service.create("Model 1")
        service.create("Model 2")

        models = service.list()

        assert models[0].id == 1
        assert models[0].name == "Model 1"
        assert models[0].created_by == "@unknown"
        assert models[0].created_at == fake_time.replace(tzinfo=None)

        assert models[1].id == 2
        assert models[1].name == "Model 2"
        assert models[1].created_by == "@unknown"
        assert models[1].created_at == fake_time.replace(tzinfo=None)


class TestModelTabulate(ModelServiceTest):
    def test_model_tabulate(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        service.create("Model 1")
        service.create("Model 2")

        expected_models = pd.DataFrame(
            [
                [
                    1,
                    "Model 1",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "Model 2",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

        models = service.tabulate()
        pdt.assert_frame_equal(models, expected_models, check_like=True)


# TODO: refactor to a filter test class for efficiency
# def test_filter_model(self, platform: ixmp4.Platform) -> None:
#     run1, run2 = self.filter.load_dataset(platform)

#     res = platform.backend.models.tabulate(
#         iamc={
#             "run": {"model": {"name": "Model 1"}},
#             "unit": {"name": "Unit 1"},
#         }
#     )
#     assert sorted(res["name"].tolist()) == ["Model 1", "Model 3"]

#     run2.set_as_default()
#     res = platform.backend.models.tabulate(
#         iamc={
#             "variable": {"name__in": ["Variable 3", "Variable 5"]},
#         }
#     )
#     assert sorted(res["name"].tolist()) == ["Model 2", "Model 3"]

#     run2.unset_as_default()
#     res = platform.backend.models.tabulate(
#         iamc={
#             "variable": {"name__like": "Variable *"},
#             "unit": {"name__in": ["Unit 1", "Unit 3"]},
#             "run": {
#                 "model": {"name__in": ["Model 1", "Model 2"]},
#                 "default_only": True,
#             },
#         }
#     )
#     assert res["name"].tolist() == ["Model 1", "Model 3"]

#     res = platform.backend.models.tabulate(
#         iamc={
#             "variable": {"name__like": "Variable *"},
#             "unit": {"name__in": ["Unit 1", "Unit 3"]},
#             "run": {
#                 "model": {"name__in": ["Model 1", "Model 2"]},
#                 "default_only": False,
#             },
#         }
#     )
#     assert sorted(res["name"].tolist()) == [
#         "Model 1",
#         "Model 2",
#         "Model 3",
#         "Model 4",
#     ]

#     res = platform.backend.models.tabulate(iamc=False)

#     assert res["name"].tolist() == ["Model 5"]

#     res = platform.backend.models.tabulate()

#     assert sorted(res["name"].tolist()) == [
#         "Model 1",
#         "Model 2",
#         "Model 3",
#         "Model 4",
#         "Model 5",
#     ]
