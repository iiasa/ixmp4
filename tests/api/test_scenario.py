from ixmp4.data.api import DataFrame
from ..utils import (
    api_platforms,
    create_iamc_query_test_data,
    assert_unordered_equality,
)


@api_platforms
def test_index_scenario(test_mp):
    table_endpoint = "iamc/scenarios/?table=True"
    _, _ = create_iamc_query_test_data(test_mp)

    res = test_mp.backend.client.patch(table_endpoint)
    res2 = test_mp.backend.client.get(table_endpoint)
    df = DataFrame(**res.json()).to_pandas()
    df2 = DataFrame(**res2.json()).to_pandas()
    assert_unordered_equality(df, df2)

    assert len(res.json()["data"]) == 2

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"model": {"name__in": ["Model 1"]}}}
    )

    assert res.json()["data"][0][0] == "Scenario 1"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"run": {"model": {"name": "Model 2"}}}
    )

    assert res.json()["data"][0][0] == "Scenario 2"

    res = test_mp.backend.client.patch(
        table_endpoint, json={"variable": {"name": "Variable 4"}}
    )

    assert res.json()["data"][0][0] == "Scenario 2"
