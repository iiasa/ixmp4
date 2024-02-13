import random
import sys
from datetime import datetime, timedelta
from itertools import cycle
from typing import Generator

import numpy as np
import pandas as pd

from ixmp4.core.platform import Backend, Platform
from ixmp4.core.region import Region
from ixmp4.core.run import Run
from ixmp4.core.unit import Unit
from ixmp4.data.abstract import DataPoint


class MockDataGenerator(object):
    platform: Platform
    backend: Backend

    def __init__(
        self,
        platform: Platform,
        num_models: int,
        num_runs: int,
        num_regions: int,
        num_variables: int,
        num_units: int,
        num_datapoints: int,
    ) -> None:
        self.platform = platform
        self.backend = platform.backend
        self.num_models = num_models
        self.num_runs = num_runs
        self.num_regions = num_regions
        self.num_variables = num_variables
        self.num_units = num_units
        self.num_datapoints = num_datapoints

    def yield_model_names(self):
        for i in range(self.num_models):
            yield f"Model {i}"

    def yield_runs(self, model_names: Generator[str, None, None]):
        scen_per_model = self.num_runs // self.num_models
        if scen_per_model == 0:
            scen_per_model = 1

        scenario_index = 0
        model_name = next(model_names)
        for i in range(self.num_runs):
            run = self.platform.runs.create(model_name, f"Scenario {scenario_index}")
            yield run

            scenario_index += 1
            if scenario_index == scen_per_model:
                run.set_as_default()
                model_name = next(model_names)
                scenario_index = 0

    def yield_regions(self):
        for i in range(self.num_regions):
            name = f"Region {i}"
            try:
                yield self.platform.regions.create(name, "default")
            except Region.NotUnique:
                yield self.platform.regions.get(name)

    def yield_units(self):
        for i in range(self.num_units):
            name = f"Unit {i}"
            try:
                yield self.platform.units.create(name)
            except Unit.NotUnique:
                yield self.platform.units.get(name)

    def yield_variable_names(self):
        for i in range(self.num_variables):
            yield f"Variable {i}"

    def yield_datapoints(
        self,
        runs: Generator[Run, None, None],
        variable_names: Generator[str, None, None],
        units: Generator[Unit, None, None],
        regions: Generator[Region, None, None],
    ):
        dp_count = 0
        for run in runs:
            region_name = next(regions).name
            variable_name = next(variable_names)
            unit_name = next(units).name
            dp_type = random.choice(
                [
                    DataPoint.Type.ANNUAL,
                    DataPoint.Type.CATEGORICAL,
                    DataPoint.Type.DATETIME,
                ]
            )
            df = self.get_datapoints(
                dp_type,
                max=self.num_datapoints - dp_count,
            )
            df["region"] = region_name
            df["variable"] = variable_name
            df["unit"] = unit_name
            run.iamc.add(df, type=dp_type)
            yield df
            dp_count += len(df)
            if self.num_datapoints == dp_count:
                break

    def get_datapoints(self, type: DataPoint.Type, max: int = sys.maxsize):
        df = pd.DataFrame(
            columns=[
                "region",
                "variable",
                "unit",
                "value",
            ],
        )
        if type == DataPoint.Type.ANNUAL:
            amount = min(20, max)
            start_year = random.randint(1950, 2000)
            steps_annual = [start_year + i for i in range(amount)]
            df["step_year"] = steps_annual
        if type == DataPoint.Type.CATEGORICAL:
            amount = min(50, max)
            num_categories = random.randint(2, 10)
            start_year = random.randint(1950, 2000)
            categories = cycle([f"Category {i}" for i in range(num_categories)])
            steps_year = []
            steps_category = []
            for i in range(amount):
                steps_category.append(next(categories))
                steps_year.append(start_year + (i // num_categories))

            df["step_year"] = steps_year
            df["step_category"] = steps_category
        if type == DataPoint.Type.DATETIME:
            amount = min(100, max)
            dt = timedelta(minutes=random.randint(1, 360) * 10)
            start_dt = datetime(
                year=random.randint(1950, 2000),
                month=1,
                day=1,
            ) + timedelta(days=random.randint(0, 365))
            steps_datetime = [start_dt + (dt * i) for i in range(amount)]
            df["step_datetime"] = steps_datetime

        denom = random.randint(10, 100)
        values = np.sin([(i / denom) for i in range(amount)]) * random.random() * 10
        df["value"] = values
        return df

    def generate(self):
        model_names = cycle([n for n in self.yield_model_names()])
        runs = cycle([r for r in self.yield_runs(model_names=model_names)])
        regions = cycle([r for r in self.yield_regions()])
        units = cycle([u for u in self.yield_units()])
        variable_names = cycle([v for v in self.yield_variable_names()])
        for df in self.yield_datapoints(runs, variable_names, units, regions):
            pass
