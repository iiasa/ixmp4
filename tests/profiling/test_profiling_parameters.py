import random
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import ixmp4

from ..conftest import Profiled

fixture_path = Path(__file__).parent.parent / "fixtures"


class TestOptimizationParameter:
    units = pd.read_csv(fixture_path / "small/units.csv")

    @classmethod
    def load_units(cls, platform: ixmp4.Platform) -> None:
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_indexsets(
        cls, run: ixmp4.Run, with_data: bool = False, size: str = "small"
    ) -> list[str]:
        # TODO should this always be 12? 15 is max, but should we always test 15?
        indexsets = pd.read_csv(fixture_path / "optimization/small/indexsets.csv")[
            "name"
        ][0:12].to_list()

        if with_data:
            indexsetdata = pd.read_csv(
                fixture_path / f"optimization/{size}/indexsetdata.csv"
            )["data"].to_list()
            for indexset_name in indexsets:
                run.optimization.indexsets.create(indexset_name).add(indexsetdata)
        else:
            for indexset_name in indexsets:
                run.optimization.indexsets.create(indexset_name)

        return indexsets

    @classmethod
    def load_parameter_with_data(
        cls,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        indexsets: list[str],
        size: str,
    ) -> tuple[ixmp4.Parameter, pd.DataFrame]:
        # We just need one parameter to test
        parameter = run.optimization.parameters.create(
            "Parameter 0", constrained_to_indexsets=indexsets
        )
        parameterdata = pd.read_csv(
            fixture_path / f"optimization/{size}/parameterdata.csv"
        ).iloc[:, :12]

        # Load and pick random units
        cls.load_units(platform)
        parameterdata["units"] = random.choices(
            [unit.name for unit in platform.units.list()], k=len(parameterdata)
        )

        # Set arbitrary values
        parameterdata["values"] = [float(x) for x in range(len(parameterdata))]

        return (parameter, parameterdata)

    @pytest.mark.parametrize("size", ["small", "medium"])
    def test_create_parameter(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks creating parameters."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = self.load_indexsets(run)
            parameters = pd.read_csv(
                fixture_path / f"optimization/{size}/parameters.csv"
            )
            return (), {"run": run, "parameters": parameters, "indexsets": indexsets}

        def run(run: ixmp4.Run, parameters: pd.DataFrame, indexsets: list[str]) -> None:
            with profiled():
                for _, name in parameters.itertuples():
                    run.optimization.parameters.create(
                        name, constrained_to_indexsets=indexsets
                    )

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.parametrize("size", ["small", "medium"])
    def test_get_parameter(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks getting parameters."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = self.load_indexsets(run)
            parameters = pd.read_csv(
                fixture_path / f"optimization/{size}/parameters.csv"
            )
            for _, name in parameters.itertuples():
                run.optimization.parameters.create(
                    name, constrained_to_indexsets=indexsets
                )
            return (), {"run": run, "parameters": parameters}

        def run(run: ixmp4.Run, parameters: pd.DataFrame) -> None:
            with profiled():
                for _, name in parameters.itertuples():
                    run.optimization.parameters.get(name)

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.parametrize("size", ["small", "medium"])
    def test_parameter_add_data_insert(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks adding data to parameters when data is empty before."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = self.load_indexsets(run, with_data=True, size=size)

            parameter, parameterdata = self.load_parameter_with_data(
                platform=db_platform, run=run, indexsets=indexsets, size=size
            )

            return (), {"parameter": parameter, "parameterdata": parameterdata}

        def run(parameter: ixmp4.Parameter, parameterdata: pd.DataFrame) -> None:
            with profiled():
                parameter.add(parameterdata)

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.parametrize("size", ["small", "medium"])
    def test_parameter_add_data_upsert_half(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks adding data to parameters when half the keys are present."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = self.load_indexsets(run, with_data=True, size=size)

            parameter, parameterdata = self.load_parameter_with_data(
                platform=db_platform, run=run, indexsets=indexsets, size=size
            )
            parameter.add(parameterdata)

            # Change half the values
            update_parameterdata = parameterdata.iloc[
                : int(len(parameterdata) / 2)
            ].copy(deep=True)
            update_parameterdata["values"] = [
                int(x) + 1 for x in range(len(update_parameterdata))
            ]

            # Craft new keys by swapping column names
            insert_parameterdata = parameterdata.iloc[
                int(len(parameterdata) / 2) : len(parameterdata)
            ].rename(
                {"Indexset 0": "Indexset 11", "Indexset 11": "Indexset 0"},
                axis="columns",
            )

            parameterdata = pd.concat([update_parameterdata, insert_parameterdata])

            return (), {"parameter": parameter, "parameterdata": parameterdata}

        def run(parameter: ixmp4.Parameter, parameterdata: pd.DataFrame) -> None:
            with profiled():
                parameter.add(parameterdata)

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.parametrize("size", ["small", "medium"])
    def test_parameter_add_data_upsert_all(
        self,
        db_platform: ixmp4.Platform,
        profiled: Profiled,
        # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
        # is closed
        benchmark: Any,
        size: str,
    ) -> None:
        """Benchmarks adding data to parameters when all keys are already present."""

        def setup() -> tuple[tuple[()], dict[str, object]]:
            run = db_platform.runs.create("Model", "Scenario")
            indexsets = self.load_indexsets(run, with_data=True, size=size)

            parameter, parameterdata = self.load_parameter_with_data(
                platform=db_platform, run=run, indexsets=indexsets, size=size
            )
            parameter.add(parameterdata)

            # Change all values and likely most units
            parameterdata["values"] = [int(x) + 1 for x in range(len(parameterdata))]
            parameterdata["units"] = random.choices(
                [unit.name for unit in db_platform.units.list()], k=len(parameterdata)
            )

            return (), {"parameter": parameter, "parameterdata": parameterdata}

        def run(parameter: ixmp4.Parameter, parameterdata: pd.DataFrame) -> None:
            with profiled():
                parameter.add(parameterdata)

        benchmark.pedantic(run, setup=setup)
