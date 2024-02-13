import pytest

from .utils import generated_platforms


@generated_platforms
@pytest.mark.parametrize(
    "filters",
    [
        {
            "model": {"name": "Model 0"},
            "scenario": {"name": "Scenario 0"},
            "run": {"default_only": False},
        },
        {
            "model": {"name__like": "Model *"},
        },
        {
            "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
            "run": {"default_only": False},
        },
        {"region": {"name__in": [f"Region {i}" for i in range(10)]}},
        {"variable": {"name__like": "Variable 1*"}},
        {
            "model": {"name__like": "Model *"},
            "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
            "variable": {"name__like": "Variable 1*"},
            "region": {"name__in": [f"Region {i}" for i in range(10)]},
            "run": {"default_only": False},
        },
    ],
)
def test_filter_datapoints_benchmark(
    generated_mp, profiled, benchmark, filters, request
):
    """Benchmarks a the filtration of `test_data_big`."""
    generated_mp = request.getfixturevalue(generated_mp)

    def run():
        with profiled():
            return generated_mp.iamc.tabulate(**filters)

    df = benchmark.pedantic(run, warmup_rounds=5, rounds=10)
    assert not df.empty
