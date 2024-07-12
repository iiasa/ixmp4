from typing import cast

import httpx

import ixmp4
from ixmp4.data.backend import RestBackend

from .fixtures import BigIamcDataset, SmallIamcDataset


class TestApi:
    small = SmallIamcDataset()
    big = BigIamcDataset()

    def assert_res(self, res: httpx.Response, is_success=True):
        assert res.is_success == is_success

    def assert_table_res(
        self,
        res: httpx.Response,
        no_of_rows: int | None = None,
        has_columns: list[str] | None = None,
        has_data_for_columns: dict[str, list] | None = None,
    ):
        self.assert_res(res)
        page = res.json()
        table = page["results"]
        columns = table["columns"]
        if has_data_for_columns is not None:
            has_columns = list(has_data_for_columns.keys())

        if has_columns is not None:
            for col in has_columns:
                assert col in columns, "Column does not exist in response table."

        data = table["data"]
        if no_of_rows is not None:
            assert len(data) == no_of_rows

        if has_data_for_columns is not None:
            col_lens = [len(v) for v in has_data_for_columns.values()]
            if not (min(col_lens) == max(col_lens)):
                assert False, "`has_data_for_columns` must have consistent length"
            else:
                assert len(data) == max(col_lens)
            for col_name, expected_data in has_data_for_columns.items():
                index = columns.index(col_name)
                res_column_data = [row[index] for row in data]
                assert expected_data == res_column_data

    def assert_paginated_res(
        self,
        client,
        endpoint,
        filters: dict | None = None,
        no_of_rows: int | None = None,
    ):
        total, offset, limit = None, None, None
        ret_no_of_rows = 0

        while offset is None or offset + limit < total:
            url = endpoint + "?table=true"
            if offset is not None:
                offset += limit
                url += f"&offset={offset}&limit={limit}"
            res = client.patch(url, json=filters)
            self.assert_res(res)
            page = res.json()
            pagination = page.pop("pagination")
            offset, limit = pagination["offset"], pagination["limit"]
            total = page.pop("total")
            table = page["results"]
            data = table["data"]
            page_no_of_rows = len(data)
            ret_no_of_rows += page_no_of_rows
            num_expected = min(total - offset, limit)
            assert page_no_of_rows == num_expected

        if no_of_rows is not None:
            assert no_of_rows == ret_no_of_rows

    def test_index_meta(self, rest_platform: ixmp4.Platform):
        self.small.load_dataset(rest_platform)
        backend = cast(RestBackend, rest_platform.backend)

        res = backend.client.patch("meta/?table=true")
        self.assert_table_res(
            res,
            no_of_rows=6,
            has_columns=["run__id", "key", "type"],
        )

        res = backend.client.patch("meta/?table=true&join_run_index=true")
        self.assert_table_res(
            res,
            no_of_rows=6,
            has_columns=["model", "scenario", "version"],
        )

        res = backend.client.patch("meta/?table=false&join_run_index=true")
        assert res.status_code == 400

    def test_index_model(self, rest_platform: ixmp4.Platform):
        self.small.load_dataset(rest_platform)
        backend = cast(RestBackend, rest_platform.backend)
        table_endpoint = "iamc/models/?table=True"

        res = backend.client.patch(table_endpoint)
        self.assert_table_res(res, no_of_rows=2, has_columns=["name"])

        res = backend.client.patch(
            table_endpoint, json={"run": {"scenario": {"name__in": ["Scenario 1"]}}}
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"name": ["Model 1"]},
        )

        res = backend.client.patch(
            table_endpoint, json={"run": {"scenario": {"name": "Scenario 2"}}}
        )

        self.assert_table_res(
            res,
            has_data_for_columns={"name": ["Model 2"]},
        )

        res = backend.client.patch(
            table_endpoint, json={"variable": {"name": "Variable 4"}}
        )

        self.assert_table_res(
            res,
            has_data_for_columns={"name": ["Model 2"]},
        )

    def test_index_scenario(self, rest_platform: ixmp4.Platform):
        self.small.load_dataset(rest_platform)
        backend = cast(RestBackend, rest_platform.backend)
        table_endpoint = "iamc/scenarios/?table=True"

        res = backend.client.patch(table_endpoint)
        self.assert_table_res(
            res,
            no_of_rows=2,
            has_columns=["name"],
        )

        res = backend.client.patch(
            table_endpoint, json={"run": {"model": {"name__in": ["Model 1"]}}}
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"name": ["Scenario 1"]},
        )

        res = backend.client.patch(
            table_endpoint, json={"run": {"model": {"name": "Model 2"}}}
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"name": ["Scenario 2"]},
        )

        res = backend.client.patch(
            table_endpoint, json={"variable": {"name": "Variable 4"}}
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"name": ["Scenario 2"]},
        )

    def test_index_region(self, rest_platform: ixmp4.Platform):
        self.small.load_dataset(rest_platform)
        backend = cast(RestBackend, rest_platform.backend)
        table_endpoint = "iamc/regions/?table=True"

        res = backend.client.patch(table_endpoint)
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [1, 2, 3]},
        )
        res = backend.client.patch(
            table_endpoint, json={"unit": {"name__in": ["Unit 1", "Unit 2"]}}
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [1, 2]},
        )

        res = backend.client.patch(
            table_endpoint,
            json={"variable": {"name__in": ["Variable 2", "Variable 3"]}},
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [2, 3]},
        )

        res = backend.client.patch(
            table_endpoint,
            json={
                "unit": {"name__in": ["Unit 1", "Unit 2"]},
                "variable": {"name__in": ["Variable 2", "Variable 3"]},
            },
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [2]},
        )

    def test_index_unit(self, rest_platform: ixmp4.Platform):
        self.small.load_dataset(rest_platform)
        backend = cast(RestBackend, rest_platform.backend)
        table_endpoint = "iamc/units/?table=True"

        res = backend.client.patch(table_endpoint)
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [1, 2, 3]},
        )

        res = backend.client.patch(
            table_endpoint, json={"region": {"name__in": ["Region 1", "Region 2"]}}
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [1, 2]},
        )

        res = backend.client.patch(
            table_endpoint,
            json={"variable": {"name__in": ["Variable 2", "Variable 3"]}},
        )
        self.assert_table_res(
            res,
            has_data_for_columns={"id": [2, 3]},
        )

        res = backend.client.patch(
            table_endpoint,
            json={
                "region": {"name__in": ["Region 1", "Region 2"]},
                "variable": {"name__in": ["Variable 2", "Variable 3"]},
            },
        )

        self.assert_table_res(
            res,
            has_data_for_columns={"id": [2]},
        )

    def test_paginate_datapoints(self, rest_platform: ixmp4.Platform):
        self.big.load_dataset(rest_platform)
        client = cast(RestBackend, rest_platform.backend).client
        endpoint = "iamc/datapoints/"
        filters = {"run": {"default_only": False}}
        self.assert_paginated_res(
            client,
            endpoint,
            filters=filters,
            no_of_rows=len(self.big.datapoints),
        )
