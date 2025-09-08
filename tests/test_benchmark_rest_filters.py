from typing import Any, cast

import pytest

import ixmp4
from ixmp4.data.backend.api import RestBackend

from .conftest import Profiled

WARMUP_ROUNDS = 5
ROUNDS = 10


class TestBenchmarkRestFilters:
    """Benchmark tests for various repository filters using big dataset."""

    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"name": "Model 0"},
            {"iamc": True},
            {
                "iamc": {"variable": {"name__like": "Variable 1*"}},
            },
            {
                "iamc": {
                    "scenario": {"name": "Scenario 0"},
                    "run": {"default_only": False},
                },
            },
            {
                "name__like": "Model *",
                "iamc": {
                    "scenario": {"name__like": "Scenario *"},
                    "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
                    "variable": {"name__like": "Variable 1*"},
                    "region": {"name__in": [f"Region {i}" for i in range(10)]},
                    "run": {"default_only": False},
                },
            },
        ],
    )
    @pytest.mark.benchmark
    def test_models_repository_benchmark(
        self,
        rest_platform_big: ixmp4.Platform,
        profiled: Profiled,
        benchmark: Any,
        filters: dict[str, Any],
    ) -> None:
        """Benchmarks the models repository with various filters."""

        client = cast(RestBackend, rest_platform_big.backend).client
        endpoint = "models/"

        def run() -> Any:
            with profiled():
                return client.get(endpoint, params=filters).json()

        df = benchmark.pedantic(run, warmup_rounds=WARMUP_ROUNDS, rounds=ROUNDS)
        assert df is not None

    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"name": "Scenario 0"},
            {"iamc": True},
            {
                "iamc": {"variable": {"name__like": "Variable 1*"}},
            },
            {
                "iamc": {
                    "model": {"name__like": "Model *"},
                    "run": {"default_only": False},
                },
            },
            {
                "name__like": "Scenario *",
                "iamc": {
                    "model": {"name__like": "Model *"},
                    "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
                    "variable": {"name__like": "Variable 1*"},
                    "region": {"name__in": [f"Region {i}" for i in range(10)]},
                    "run": {"default_only": False},
                },
            },
        ],
    )
    @pytest.mark.benchmark
    def test_scenarios_repository_benchmark(
        self,
        rest_platform_big: ixmp4.Platform,
        profiled: Profiled,
        benchmark: Any,
        filters: dict[str, Any],
    ) -> None:
        """Benchmarks the scenarios repository with various filters."""

        client = cast(RestBackend, rest_platform_big.backend).client
        endpoint = "scenarios/"

        def run() -> Any:
            with profiled():
                return client.get(endpoint, params=filters).json()

        df = benchmark.pedantic(run, warmup_rounds=WARMUP_ROUNDS, rounds=ROUNDS)
        assert df is not None

    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"model": {"name": "Model 0"}},
            {"scenario": {"name": "Scenario 0"}},
            {"iamc": True},
            {
                "iamc": {"variable": {"name__like": "Variable 1*"}},
            },
            {
                "iamc": {
                    "scenario": {"name": "Scenario 0"},
                    "model": {"name__like": "Model *"},
                    "run": {"default_only": False},
                },
            },
            {
                "default_only": False,
                "iamc": {
                    "model": {"name__like": "Model *"},
                    "scenario": {"name__like": "Scenario *"},
                    "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
                    "variable": {"name__like": "Variable 1*"},
                    "region": {"name__in": [f"Region {i}" for i in range(10)]},
                    "run": {"default_only": False},
                },
            },
        ],
    )
    @pytest.mark.benchmark
    def test_runs_repository_benchmark(
        self,
        rest_platform_big: ixmp4.Platform,
        profiled: Profiled,
        benchmark: Any,
        filters: dict[str, Any],
    ) -> None:
        """Benchmarks the runs repository with various filters."""

        client = cast(RestBackend, rest_platform_big.backend).client
        endpoint = "runs/"

        def run() -> Any:
            with profiled():
                return client.get(endpoint, params=filters).json()

        df = benchmark.pedantic(run, warmup_rounds=WARMUP_ROUNDS, rounds=ROUNDS)
        assert df is not None

    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"name": "Region 0"},
            {"iamc": True},
            {
                "iamc": {"variable": {"name__like": "Variable 1*"}},
            },
            {
                "iamc": {
                    "scenario": {"name": "Scenario 0"},
                    "model": {"name__like": "Model *"},
                    "run": {"default_only": False},
                },
            },
            {
                "name__in": [f"Region {i}" for i in range(10)],
                "iamc": {
                    "model": {"name__like": "Model *"},
                    "scenario": {"name__like": "Scenario *"},
                    "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
                    "variable": {"name__like": "Variable 1*"},
                    "run": {"default_only": False},
                },
            },
        ],
    )
    @pytest.mark.benchmark
    def test_regions_repository_benchmark(
        self,
        rest_platform_big: ixmp4.Platform,
        profiled: Profiled,
        benchmark: Any,
        filters: dict[str, Any],
    ) -> None:
        """Benchmarks the regions repository with various filters."""

        client = cast(RestBackend, rest_platform_big.backend).client
        endpoint = "regions/"

        def run() -> Any:
            with profiled():
                return client.get(endpoint, params=filters).json()

        df = benchmark.pedantic(run, warmup_rounds=WARMUP_ROUNDS, rounds=ROUNDS)
        assert df is not None

    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"name": "Unit 0"},
            {"iamc": True},
            {
                "iamc": {"variable": {"name__like": "Variable 1*"}},
            },
            {
                "iamc": {
                    "scenario": {"name": "Scenario 0"},
                    "model": {"name__like": "Model *"},
                    "run": {"default_only": False},
                },
            },
            {
                "name__in": [f"Unit {i}" for i in range(10)],
                "iamc": {
                    "model": {"name__like": "Model *"},
                    "scenario": {"name__like": "Scenario *"},
                    "variable": {"name__like": "Variable 1*"},
                    "region": {"name__in": [f"Region {i}" for i in range(10)]},
                    "run": {"default_only": False},
                },
            },
        ],
    )
    @pytest.mark.benchmark
    def test_units_repository_benchmark(
        self,
        rest_platform_big: ixmp4.Platform,
        profiled: Profiled,
        benchmark: Any,
        filters: dict[str, Any],
    ) -> None:
        """Benchmarks the units repository with various filters."""

        client = cast(RestBackend, rest_platform_big.backend).client
        endpoint = "units/"

        def run() -> Any:
            with profiled():
                return client.get(endpoint, params=filters).json()

        df = benchmark.pedantic(run, warmup_rounds=WARMUP_ROUNDS, rounds=ROUNDS)
        assert df is not None

    @pytest.mark.parametrize(
        "filters",
        [
            {},
            {"name": "Variable 0"},
            {"name__like": "Variable *"},
            {"unit": {"name": "Unit 0"}},
            {
                "name__like": "Variable *",
                "scenario": {"name": "Scenario 0"},
                "model": {"name__like": "Model *"},
                "run": {"default_only": False},
            },
            {
                "name__like": "Variable *",
                "model": {"name__like": "Model *"},
                "scenario": {"name__like": "Scenario *"},
                "variable": {"name__like": "Variable 1*"},
                "region": {"name__in": [f"Region {i}" for i in range(10)]},
                "run": {"default_only": False},
            },
        ],
    )
    @pytest.mark.benchmark
    def test_variables_repository_benchmark(
        self,
        rest_platform_big: ixmp4.Platform,
        profiled: Profiled,
        benchmark: Any,
        filters: dict[str, Any],
    ) -> None:
        """Benchmarks the variables repository with various filters."""

        client = cast(RestBackend, rest_platform_big.backend).client
        endpoint = "iamc/variables/"

        def run() -> Any:
            with profiled():
                return client.get(endpoint, params=filters).json()

        df = benchmark.pedantic(run, warmup_rounds=WARMUP_ROUNDS, rounds=ROUNDS)
        assert df is not None
