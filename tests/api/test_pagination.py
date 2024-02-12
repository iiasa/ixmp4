import pytest

from ..conftest import gen_obj_nums
from ..utils import generated_api_platforms


@generated_api_platforms
@pytest.mark.parametrize(
    "endpoint,filters,total",
    [
        (
            "models/",
            {"iamc": {"run": {"default_only": False}}},
            gen_obj_nums["num_models"],
        ),
        ("runs/", {"default_only": False}, gen_obj_nums["num_runs"]),
        (
            "regions/",
            None,
            gen_obj_nums["num_regions"],
        ),
        (
            "units/",
            {"iamc": {"run": {"default_only": False}}},
            gen_obj_nums["num_units"],
        ),
        (
            "iamc/datapoints/",
            {"run": {"default_only": False}},
            gen_obj_nums["num_datapoints"],
        ),
    ],
)
def test_pagination(generated_mp, endpoint, filters, total, request):
    generated_mp = request.getfixturevalue(generated_mp)
    client = generated_mp.backend.client
    offset, limit = None, None
    while offset is None or offset + limit < total:
        url = endpoint + "?table=true"
        if offset is not None:
            url += f"&offset={offset}&limit={limit}"
        res = client.patch(url, json=filters)
        data = res.json()
        pagination = data.pop("pagination")
        offset, limit = pagination["offset"], pagination["limit"]
        ret_total = data.pop("total")
        num_expected = min(total - offset, limit)
        num_ret = len(data["results"]["index"])
        assert num_ret == num_expected
        assert ret_total == total
        offset += limit
