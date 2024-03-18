from ..utils import (
    api_platforms,
    create_iamc_query_test_data,
)


@api_platforms
def test_index_model(test_mp, request):
    test_mp = request.getfixturevalue(test_mp)
    _, _ = create_iamc_query_test_data(test_mp)

    res = test_mp.backend.client.patch("meta/?table=true")
    results = res.json()["results"]
    res_data = results["data"]

    assert len(res_data) == 6

    res = test_mp.backend.client.patch("meta/?table=true&join_run_index=true")
    results = res.json()["results"]
    res_columns = results["columns"]
    res_data = results["data"]

    assert len(res_data) == 6
    assert all(x in res_columns for x in ["model", "scenario", "version"])

    res = test_mp.backend.client.patch("meta/?table=false&join_run_index=true")
    assert res.status_code == 400
