from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import ixmp4
import ixmp4.core
import ixmp4.core.optimization
import ixmp4.core.optimization.indexset

from ..conftest import Profiled

fixture_path = Path(__file__).parent.parent / "fixtures"


class TestOptimizationIndexset:
    @pytest.mark.parametrize("size", ["small", "medium", "big"])
    def test_create_indexset(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks creating indexsets."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = pd.read_csv(fixture_path / f"optimization/{size}/indexsets.csv")
            return (), {"run": run, "indexsets": indexsets}

        def run(run: ixmp4.Run, indexsets: pd.DataFrame) -> None:
            with profiled():
                for _, name in indexsets.itertuples():
                    run.optimization.indexsets.create(name)

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.parametrize("size", ["small", "medium", "big"])
    def test_get_indexset(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks getting indexsets."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = pd.read_csv(fixture_path / f"optimization/{size}/indexsets.csv")
            for _, name in indexsets.itertuples():
                run.optimization.indexsets.create(name)
            return (), {"run": run, "indexsets": indexsets}

        def run(run: ixmp4.Run, indexsets: pd.DataFrame) -> None:
            with profiled():
                for _, name in indexsets.itertuples():
                    run.optimization.indexsets.get(name)

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.parametrize("size", ["small", "medium", "big"])
    def test_indexset_add_data(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks adding data to indexsets."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            # We just need one indexset to add data to
            indexset = run.optimization.indexsets.create("Indexset 0")
            indexsetdata = pd.read_csv(
                fixture_path / f"optimization/{size}/indexsetdata.csv"
            )["data"].to_list()
            return (), {"indexset": indexset, "indexsetdata": indexsetdata}

        def run(indexset: ixmp4.core.IndexSet, indexsetdata: list[int]) -> None:
            with profiled():
                indexset.add(indexsetdata)

        benchmark.pedantic(run, setup=setup)
