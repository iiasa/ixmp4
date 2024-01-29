from ..utils import (
    api_platforms,
    create_iamc_query_test_data,
)


@api_platforms
def test_index_model(test_mp, request):
    test_mp = request.getfixturevalue(test_mp)
    table_endpoint = "iamc/models/?table=True"
    _, _ = create_iamc_query_test_data(test_mp)

    res = test_mp.backend.client.patch(table_endpoint)

    assert len(res.json()["results"]["data"]) == 2

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"scenario": {"name__in": ["Scenario 1"]}}}
    )

    assert res.json()["results"]["data"][0][0] == "Model 1"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"scenario": {"name": "Scenario 2"}}}
    )

    assert res.json()["results"]["data"][0][0] == "Model 2"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"variable": {"name": "Variable 4"}}
    )

    assert res.json()["results"]["data"][0][0] == "Model 2"
