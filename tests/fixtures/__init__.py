from pathlib import Path

import pandas as pd

import ixmp4
from ixmp4.core.exceptions import ProgrammingError

here = Path(__file__).parent


class SmallIamcDataset:
    units = pd.read_csv(here / "small/units.csv")
    regions = pd.read_csv(here / "small/regions.csv")
    annual = pd.read_csv(here / "small/annual.csv")
    categorical = pd.read_csv(here / "small/categorical.csv")
    datetime = pd.read_csv(here / "small/datetime.csv")
    datetime["step_datetime"] = pd.to_datetime(datetime["step_datetime"])

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

        datapoints = cls.annual.copy()
        run1.iamc.add(datapoints, type=ixmp4.DataPoint.Type.ANNUAL)
        run1.meta = {"run": 1, "test": 0.1293, "bool": True}

        datapoints["variable"] = "Variable 4"
        run2.iamc.add(datapoints, type=ixmp4.DataPoint.Type.ANNUAL)
        run2.meta = {"run": 2, "test": "string", "bool": False}
        return run1, run2


class FilterIamcDataset:
    units = pd.read_csv(here / "filters/units.csv")
    regions = pd.read_csv(here / "filters/regions.csv")
    datapoints = pd.read_csv(here / "filters/datapoints.csv")

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

        dps = cls.datapoints.copy()
        run1.iamc.add(dps[dps["model"] == "Model 1"], type=ixmp4.DataPoint.Type.ANNUAL)
        run2.iamc.add(dps[dps["model"] == "Model 2"], type=ixmp4.DataPoint.Type.ANNUAL)
        return run1, run2


class MediumIamcDataset:
    runs = pd.read_csv(here / "medium/runs.csv")
    units = pd.read_csv(here / "medium/units.csv")
    regions = pd.read_csv(here / "medium/regions.csv")
    datapoints = pd.read_csv(here / "medium/datapoints.csv")
    run_cols = ["model", "scenario", "version"]

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
    def load_run_datapoints(
        cls, platform: ixmp4.Platform, run_tup: tuple[str, str, int], dps: pd.DataFrame
    ):
        run = platform.runs.get(*run_tup)

        annual = dps[dps["type"] == "ANNUAL"].dropna(how="all", axis="columns")
        categorical = dps[dps["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )
        datetime = dps[dps["type"] == "DATETIME"].dropna(how="all", axis="columns")
        if not annual.empty:
            run.iamc.add(annual, type=ixmp4.DataPoint.Type.ANNUAL)
        if not categorical.empty:
            run.iamc.add(categorical, type=ixmp4.DataPoint.Type.CATEGORICAL)
        if not datetime.empty:
            run.iamc.add(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    @classmethod
    def get_run_dps(cls, df: pd.DataFrame, model, scenario, version):
        dps = df.copy()
        dps = dps[dps["model"] == model]
        dps = dps[dps["scenario"] == scenario]
        dps = dps[dps["version"] == version]
        dps = dps.drop(columns=cls.run_cols)
        return dps

    @classmethod
    def load_dp_df(cls, platform: ixmp4.Platform, df: pd.DataFrame):
        runs = df[cls.run_cols].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            dps = cls.get_run_dps(df, model, scenario, version)
            cls.load_run_datapoints(platform, (model, scenario, version), dps)

    @classmethod
    def load_datapoints(cls, platform: ixmp4.Platform):
        cls.load_dp_df(platform, cls.datapoints)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform):
        cls.load_regions(platform)
        cls.load_units(platform)
        cls.load_runs(platform)
        cls.load_datapoints(platform)


class BigIamcDataset:
    runs = pd.read_csv(here / "big/runs.csv")
    units = pd.read_csv(here / "big/units.csv")
    regions = pd.read_csv(here / "big/regions.csv")
    datapoints = pd.read_csv(here / "big/datapoints.csv")
    run_cols = ["model", "scenario", "version"]

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
    def load_dp_df(cls, platform: ixmp4.Platform, df: pd.DataFrame):
        runs = df[cls.run_cols].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            dps = cls.get_run_dps(df, model, scenario, version)
            cls.load_run_datapoints(platform, (model, scenario, version), dps)

    @classmethod
    def get_run_dps(cls, df: pd.DataFrame, model, scenario, version):
        dps = df.copy()
        dps = dps[dps["model"] == model]
        dps = dps[dps["scenario"] == scenario]
        dps = dps[dps["version"] == version]
        dps = dps.drop(columns=cls.run_cols)
        return dps

    @classmethod
    def rm_dp_df(cls, platform: ixmp4.Platform, df: pd.DataFrame):
        runs = df[cls.run_cols].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            dps = cls.get_run_dps(df, model, scenario, version)
            cls.rm_run_datapoints(platform, (model, scenario, version), dps)

    @classmethod
    def load_datapoints(cls, platform: ixmp4.Platform):
        cls.load_dp_df(platform, cls.datapoints)

    @classmethod
    def load_datapoints_half(cls, platform: ixmp4.Platform):
        scrambled_dps = cls.datapoints.sample(frac=1)
        half_dps = scrambled_dps.head(len(scrambled_dps) // 2)
        half_dps = half_dps.sort_values(by=cls.run_cols)
        cls.load_dp_df(platform, half_dps)

    @classmethod
    def load_run_datapoints(
        cls, platform: ixmp4.Platform, run_tup: tuple[str, str, int], dps: pd.DataFrame
    ):
        run = platform.runs.get(*run_tup)

        annual = dps[dps["type"] == "ANNUAL"].dropna(how="all", axis="columns")
        categorical = dps[dps["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )
        datetime = dps[dps["type"] == "DATETIME"].dropna(how="all", axis="columns")
        run.iamc.add(annual, type=ixmp4.DataPoint.Type.ANNUAL)
        run.iamc.add(categorical, type=ixmp4.DataPoint.Type.CATEGORICAL)
        run.iamc.add(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    @classmethod
    def rm_run_datapoints(
        cls, platform: ixmp4.Platform, run_tup: tuple[str, str, int], dps: pd.DataFrame
    ):
        run = platform.runs.get(*run_tup)

        annual = dps[dps["type"] == "ANNUAL"].dropna(how="all", axis="columns")
        categorical = dps[dps["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )
        datetime = dps[dps["type"] == "DATETIME"].dropna(how="all", axis="columns")
        run.iamc.remove(annual, type=ixmp4.DataPoint.Type.ANNUAL)
        run.iamc.remove(categorical, type=ixmp4.DataPoint.Type.CATEGORICAL)
        run.iamc.remove(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform):
        cls.load_regions(platform)
        cls.load_units(platform)
        cls.load_runs(platform)
        cls.load_datapoints(platform)
