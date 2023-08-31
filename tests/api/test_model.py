from ixmp4.data.api import DataFrame

from ..utils import (
    api_platforms,
    assert_unordered_equality,
    create_iamc_query_test_data,
)


@api_platforms
def test_index_model(test_mp):
    table_endpoint = "iamc/models/?table=True"
    _, _ = create_iamc_query_test_data(test_mp)

    res = test_mp.backend.client.patch(table_endpoint)
    res2 = test_mp.backend.client.get(table_endpoint)
    df = DataFrame(**res.json()).to_pandas()
    df2 = DataFrame(**res2.json()).to_pandas()
    assert_unordered_equality(df, df2)

    assert len(res.json()["data"]) == 2

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"scenario": {"name__in": ["Scenario 1"]}}}
    )

    assert res.json()["data"][0][0] == "Model 1"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"scenario": {"name": "Scenario 2"}}}
    )

    assert res.json()["data"][0][0] == "Model 2"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"variable": {"name": "Variable 4"}}
    )

    assert res.json()["data"][0][0] == "Model 2"
