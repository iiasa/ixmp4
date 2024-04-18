from ixmp4 import Platform

from ..utils import (
    api_platforms,
    create_iamc_query_test_data,
)


@api_platforms
def test_index_scenario(test_mp, request):
    test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
    table_endpoint = "iamc/scenarios/?table=True"
    _, _ = create_iamc_query_test_data(test_mp)

    res = test_mp.backend.client.patch(table_endpoint)

    assert len(res.json()["results"]["data"]) == 2

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"model": {"name__in": ["Model 1"]}}}
    )

    assert res.json()["results"]["data"][0][1] == "Scenario 1"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"model": {"name": "Model 2"}}}
    )

    assert res.json()["results"]["data"][0][1] == "Scenario 2"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"variable": {"name": "Variable 4"}}
    )

    assert res.json()["results"]["data"][0][1] == "Scenario 2"
