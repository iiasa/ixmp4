import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.region.exceptions import RegionNotFound, RegionNotUnique
from ixmp4.data.region.service import RegionService
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class RegionServiceTest(ServiceTest[RegionService]):
    service_class = RegionService


class TestRegionCreate(RegionServiceTest):
    def test_region_create(
        self, service: RegionService, fake_time: datetime.datetime
    ) -> None:
        region = service.create("Region", "Hierarchy")
        assert region.name == "Region"
        assert region.hierarchy == "Hierarchy"
        assert region.created_at == fake_time.replace(tzinfo=None)
        assert region.created_by == "@unknown"

    def test_region_create_versioning(
        self, versioning_service: RegionService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Region",
                    "Hierarchy",
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
                "hierarchy",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()

        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestRegionDeleteById(RegionServiceTest):
    def test_region_delete_by_id(
        self, service: RegionService, fake_time: datetime.datetime
    ) -> None:
        region = service.create("Region", "Hierarchy")
        service.delete_by_id(region.id)
        assert service.tabulate().empty

    def test_region_delete_by_id_versioning(
        self, versioning_service: RegionService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Region",
                    "Hierarchy",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    2,
                    0,
                ],
                [
                    1,
                    "Region",
                    "Hierarchy",
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
                "hierarchy",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestRegionUnique(RegionServiceTest):
    def test_region_unique(self, service: RegionService) -> None:
        service.create("Region", "Hierarchy")

        with pytest.raises(RegionNotUnique):
            service.create("Region", "Hierarchy")

        with pytest.raises(RegionNotUnique):
            service.create("Region", "Another Hierarchy")


class TestRegionGetByName(RegionServiceTest):
    def test_region_get_by_name(self, service: RegionService) -> None:
        region1 = service.create("Region", "Hierarchy")
        region2 = service.get_by_name("Region")
        assert region1 == region2


class TestRegionGetById(RegionServiceTest):
    def test_region_get_by_id(self, service: RegionService) -> None:
        region1 = service.create("Region", "Hierarchy")
        region2 = service.get_by_id(1)
        assert region1 == region2


class TestRegionNotFound(RegionServiceTest):
    def test_region_not_found(self, service: RegionService) -> None:
        with pytest.raises(RegionNotFound):
            service.get_by_name("Region")

        with pytest.raises(RegionNotFound):
            service.get_by_id(1)


class TestRegionGetOrCreate(RegionServiceTest):
    def test_get_or_create_region(
        self, service: RegionService, fake_time: datetime.datetime
    ) -> None:
        region1 = service.create("Region", "Hierarchy")
        region2 = service.get_or_create("Region")
        assert region1.id == region2.id

        other_region = service.get_or_create("Other", hierarchy="Hierarchy")

        with pytest.raises(RegionNotUnique):
            service.get_or_create(other_region.name, hierarchy="Different Hierarchy")

    def test_region_get_or_create_versioning(
        self, versioning_service: RegionService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Region",
                    "Hierarchy",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    None,
                    0,
                ],
                [
                    2,
                    "Other",
                    "Hierarchy",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    2,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "name",
                "hierarchy",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestRegionList(RegionServiceTest):
    def test_region_list(
        self, service: RegionService, fake_time: datetime.datetime
    ) -> None:
        service.create("Region 1", "Hierarchy")
        service.create("Region 2", "Hierarchy")

        regions = service.list()

        assert regions[0].id == 1
        assert regions[0].name == "Region 1"
        assert regions[0].hierarchy == "Hierarchy"
        assert regions[0].created_by == "@unknown"
        assert regions[0].created_at == fake_time.replace(tzinfo=None)

        assert regions[1].id == 2
        assert regions[1].name == "Region 2"
        assert regions[1].hierarchy == "Hierarchy"
        assert regions[1].created_by == "@unknown"
        assert regions[1].created_at == fake_time.replace(tzinfo=None)


class TestRegionTabulate(RegionServiceTest):
    def test_region_tabulate(
        self, service: RegionService, fake_time: datetime.datetime
    ) -> None:
        service.create("Region 1", "Hierarchy")
        service.create("Region 2", "Hierarchy")

        expected_regions = pd.DataFrame(
            [
                [
                    1,
                    "Region 1",
                    "Hierarchy",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "Region 2",
                    "Hierarchy",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "hierarchy", "created_at", "created_by"],
        )

        regions = service.tabulate()
        pdt.assert_frame_equal(regions, expected_regions, check_like=True)


# TODO: refactor to a filter test class for efficiency
# def test_filter_region(self, platform: ixmp4.Platform) -> None:
#     run1, run2 = self.filter.load_dataset(platform)

#     res = platform.backend.regions.tabulate(
#         iamc={
#             "run": {"model": {"name": "Model 1"}},
#             "unit": {"name": "Unit 1"},
#         }
#     )
#     assert sorted(res["name"].tolist()) == ["Region 1", "Region 3"]

#     run2.set_as_default()
#     res = platform.backend.regions.tabulate(
#         iamc={
#             "variable": {"name__in": ["Variable 3", "Variable 5"]},
#         }
#     )
#     assert sorted(res["name"].tolist()) == ["Region 2", "Region 3"]

#     run2.unset_as_default()
#     res = platform.backend.regions.tabulate(
#         iamc={
#             "variable": {"name__like": "Variable *"},
#             "unit": {"name__in": ["Unit 1", "Unit 3"]},
#             "run": {
#                 "model": {"name__in": ["Model 1", "Model 2"]},
#                 "default_only": True,
#             },
#         }
#     )
#     assert res["name"].tolist() == ["Region 1", "Region 3"]

#     res = platform.backend.regions.tabulate(
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
#         "Region 1",
#         "Region 2",
#         "Region 3",
#         "Region 4",
#     ]

#     res = platform.backend.regions.tabulate(iamc=False)

#     assert res["name"].tolist() == ["Region 5"]

#     res = platform.backend.regions.tabulate()

#     assert sorted(res["name"].tolist()) == [
#         "Region 1",
#         "Region 2",
#         "Region 3",
#         "Region 4",
#         "Region 5",
#     ]
