from ixmp4.data.api import DataFrame
from ..utils import (
    api_platforms,
    create_iamc_query_test_data,
    assert_unordered_equality,
)


@api_platforms
def test_index_region(test_mp):
    table_endpoint = "iamc/regions/?table=True"
    test_triple, _ = create_iamc_query_test_data(test_mp)

    res = test_mp.backend.client.patch(table_endpoint)
    res2 = test_mp.backend.client.get(table_endpoint)
    df = DataFrame(**res.json()).to_pandas()
    df2 = DataFrame(**res2.json()).to_pandas()
    assert_unordered_equality(df, df2)

    ids = [r[0] for r in res.json()["data"]]
    assert len(ids) == 3
    assert all([id == t.id] for id, t in zip(ids, test_triple))

    res = test_mp.backend.client.patch(
        table_endpoint, json={"unit": {"name__in": ["Unit 1", "Unit 2"]}}
    )

    ids = [r[0] for r in res.json()["data"]]
    assert len(ids) == 2
    assert all([id == t.id] for id, t in zip(ids, test_triple[:2]))

    res = test_mp.backend.client.patch(
        table_endpoint,
        json={"variable": {"name__in": ["Variable 2", "Variable 3"]}},
    )

    ids = [r[0] for r in res.json()["data"]]
    assert len(ids) == 2
    assert all([id == t.id] for id, t in zip(ids, test_triple[1:]))

    res = test_mp.backend.client.patch(
        table_endpoint,
        json={
            "unit": {"name__in": ["Unit 1", "Unit 2"]},
            "variable": {"name__in": ["Variable 2", "Variable 3"]},
        },
    )
    jdf = res.json()
    [id] = [r[jdf["columns"].index("id")] for r in jdf["data"]]
    assert id == test_triple[1].id
