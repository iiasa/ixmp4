from pathlib import Path

import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import ProgrammingError

here = Path(__file__).parent


class SmallIamcDataset:
    units = pd.read_csv(here / "small/units.csv")
    regions = pd.read_csv(here / "small/regions.csv")
    datapoints = pd.read_csv(here / "small/datapoints.csv")

    @classmethod
    def load_regions(cls, platform: ixmp4.Platform):
        for _, name, hierarchy in cls.regions.itertuples():
            platform.regions.create(name, hierarchy)

    @classmethod
    def load_units(cls, platform: ixmp4.Platform):
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform):
        cls.load_regions(platform)
        cls.load_units(platform)

        # create runs
        run1 = platform.runs.create("Model 1", "Scenario 1")
        run1.set_as_default()
        run2 = platform.runs.create("Model 2", "Scenario 2")
        run2.set_as_default()

        datapoints = cls.datapoints.copy()
        run1.iamc.add(datapoints)
        run1.meta = {"run": 1, "test": 0.1293, "bool": True}

        datapoints["variable"] = "Variable 4"
        run2.iamc.add(datapoints)
        run2.meta = {"run": 2, "test": "string", "bool": False}


class BigIamcDataset:
    runs = pd.read_csv(here / "big/runs.csv")
    units = pd.read_csv(here / "big/units.csv")
    regions = pd.read_csv(here / "big/regions.csv")
    datapoints = pd.read_csv(here / "big/datapoints.csv")

    @classmethod
    def load_regions(cls, platform: ixmp4.Platform):
        for _, name, hierarchy in cls.regions.itertuples():
            platform.regions.create(name, hierarchy)

    @classmethod
    def load_units(cls, platform: ixmp4.Platform):
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_runs(cls, platform: ixmp4.Platform):
        for _, model, scenario, version, is_default in cls.runs.itertuples():
            run = platform.runs.create(model, scenario)
            if run.version != version:
                raise ProgrammingError("Run fixture incomplete or out of order.")

            if is_default:
                run.set_as_default()

    @classmethod
    def load_datapoints(cls, platform: ixmp4.Platform):
        runs = cls.datapoints[["model", "scenario", "version"]].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            cls.load_run_datapoints(platform, (model, scenario, version))

    @classmethod
    def load_run_datapoints(
        cls, platform: ixmp4.Platform, run_tup: tuple[str, str, int]
    ):
        run = platform.runs.get(*run_tup)

        dps = cls.datapoints.copy()
        dps = dps[dps["model"] == run.model.name]
        dps = dps[dps["scenario"] == run.scenario.name]
        dps = dps[dps["version"] == run.version]
        dps.drop(columns=["model", "scenario", "version"], inplace=True)

        annual = dps[dps["type"] == "ANNUAL"].dropna(how="all", axis="columns")
        categorical = dps[dps["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )
        datetime = dps[dps["type"] == "DATETIME"].dropna(how="all", axis="columns")
        run.iamc.add(annual, type=ixmp4.DataPoint.Type.ANNUAL)
        run.iamc.add(categorical, type=ixmp4.DataPoint.Type.CATEGORICAL)
        run.iamc.add(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform):
        cls.load_regions(platform)
        cls.load_units(platform)
        cls.load_runs(platform)
        cls.load_datapoints(platform)
