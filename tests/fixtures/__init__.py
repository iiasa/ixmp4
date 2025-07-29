import json
import random
import string
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

import ixmp4
from ixmp4.core.exceptions import ProgrammingError

here = Path(__file__).parent


def json_timestamp_decoder(obj: dict[Any, Any]) -> dict[Any, Any]:
    """
    Hook method that takes a dictionary and returns one in which qualifying
    have been parsed into `date` and `datetime` objects.
    """

    for key, value in obj.items():
        if not isinstance(value, str):
            continue
        try:
            obj[key] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return obj


class MigrationFixtures:
    """Provides fixtures for testing migrations."""

    @staticmethod
    def get_migration_data(id: str) -> list[dict[str, Any]]:
        """Get the migration data as a list of dictionaries."""
        with open(here / "migrations" / (id + ".json"), "r") as f:
            return cast(
                list[dict[str, Any]], json.load(f, object_hook=json_timestamp_decoder)
            )

    c71efc396d2b = get_migration_data("c71efc396d2b")


class SmallIamcDataset:
    units = pd.read_csv(here / "small/units.csv")
    regions = pd.read_csv(here / "small/regions.csv")
    annual = pd.read_csv(here / "small/annual.csv")
    categorical = pd.read_csv(here / "small/categorical.csv")
    datetime = pd.read_csv(here / "small/datetime.csv")
    datetime["step_datetime"] = pd.to_datetime(datetime["step_datetime"])
    mixed = pd.read_csv(here / "small/mixed.csv")
    mixed["step_datetime"] = pd.to_datetime(mixed["step_datetime"])

    @classmethod
    def load_regions(cls, platform: ixmp4.Platform) -> None:
        for _, name, hierarchy in cls.regions.itertuples():
            platform.regions.create(name, hierarchy)

    @classmethod
    def load_units(cls, platform: ixmp4.Platform) -> None:
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform) -> tuple[ixmp4.Run, ixmp4.Run]:
        cls.load_regions(platform)
        cls.load_units(platform)

        # create runs
        run1 = platform.runs.create("Model 1", "Scenario 1")
        run1.set_as_default()
        run2 = platform.runs.create("Model 2", "Scenario 2")
        run2.set_as_default()

        datapoints = cls.annual.copy()
        with run1.transact("Add iamc data"):
            run1.iamc.add(datapoints, type=ixmp4.DataPoint.Type.ANNUAL)
        # NOTE mypy doesn't support setters taking a different type than
        # their property https://github.com/python/mypy/issues/3004
        with run1.transact("Add meta data"):
            run1.meta = {"run": 1, "test": 0.1293, "bool": True}  # type: ignore[assignment]

        datapoints["variable"] = "Variable 4"
        with run2.transact("Add iamc data"):
            run2.iamc.add(datapoints, type=ixmp4.DataPoint.Type.ANNUAL)

        with run2.transact("Add meta data"):
            run2.meta = {"run": 2, "test": "string", "bool": False}  # type: ignore[assignment]

        return run1, run2


class FilterIamcDataset:
    units = pd.read_csv(here / "filters/units.csv")
    regions = pd.read_csv(here / "filters/regions.csv")
    datapoints = pd.read_csv(here / "filters/datapoints.csv")

    @classmethod
    def load_regions(cls, platform: ixmp4.Platform) -> None:
        for _, name, hierarchy in cls.regions.itertuples():
            platform.regions.create(name, hierarchy)

    @classmethod
    def load_units(cls, platform: ixmp4.Platform) -> None:
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform) -> tuple[ixmp4.Run, ixmp4.Run]:
        cls.load_regions(platform)
        cls.load_units(platform)

        # create runs
        run1 = platform.runs.create("Model 1", "Scenario 1")
        run1.set_as_default()
        run2 = platform.runs.create("Model 2", "Scenario 2")

        dps = cls.datapoints.copy()
        with run1.transact("Add iamc data"):
            run1.iamc.add(
                dps[dps["model"] == "Model 1"], type=ixmp4.DataPoint.Type.ANNUAL
            )

        with run2.transact("Add iamc data"):
            run2.iamc.add(
                dps[dps["model"] == "Model 2"], type=ixmp4.DataPoint.Type.ANNUAL
            )
        return run1, run2


class MediumIamcDataset:
    runs = pd.read_csv(here / "medium/runs.csv")
    units = pd.read_csv(here / "medium/units.csv")
    regions = pd.read_csv(here / "medium/regions.csv")
    datapoints = pd.read_csv(here / "medium/datapoints.csv")
    run_cols = ["model", "scenario", "version"]

    def add_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        df.insert(0, "id", range(1, len(df) + 1))
        return df

    def table_data_from_name_column(
        self, df: pd.DataFrame, name_col: str
    ) -> pd.DataFrame:
        res = pd.DataFrame({"name": df[name_col].unique()})
        res = self.add_ids(res)
        return res

    def merge_id_column(
        self, left: pd.DataFrame, right: pd.DataFrame, name_col: str, id_col: str
    ) -> pd.DataFrame:
        res = left.merge(
            right, left_on=name_col, right_on="name", suffixes=("_left", "")
        )
        res = res.rename(columns={"id": id_col})
        return res.drop(columns=["name"])

    def alembic_table_data(self) -> list[dict[Any, Any]]:
        # the things i do for 100 percent test coverage
        models = self.table_data_from_name_column(self.datapoints, "model")
        scenarios = self.table_data_from_name_column(self.datapoints, "scenario")
        regions = self.table_data_from_name_column(self.datapoints, "region")
        regions["hierarchy"] = "default"
        units = self.table_data_from_name_column(self.datapoints, "unit")
        variables = self.table_data_from_name_column(self.datapoints, "variable")
        measurands = self.datapoints[["unit", "variable"]].drop_duplicates()
        measurands = self.merge_id_column(measurands, units, "unit", "unit__id")
        measurands = self.merge_id_column(
            measurands, variables, "variable", "variable__id"
        )
        measurands = self.add_ids(measurands)

        runs = self.datapoints[["model", "scenario", "version"]].drop_duplicates()
        # join models
        runs = self.merge_id_column(runs, models, "model", "model__id")
        # join scenarios
        runs = self.merge_id_column(runs, scenarios, "scenario", "scenario__id")
        runs = self.add_ids(runs)

        types = pd.Series(["INT", "FLOAT", "STR", "BOOL"])
        n_metas = 100
        run_metas = pd.DataFrame(
            {
                "run__id": runs["id"].sample(n=n_metas, replace=True).values,
                "type": types.sample(n=n_metas, replace=True).values,
                "key": [
                    "".join(
                        random.choice(string.ascii_uppercase + string.digits)
                        for _ in range(10)
                    )
                    for _ in range(n_metas)
                ],
            }
        )
        run_metas["value_int"] = None
        run_metas["value_str"] = None
        run_metas["value_bool"] = None
        run_metas["value_float"] = None
        run_metas.loc[run_metas["type"] == "INT", "value_int"] = 13
        run_metas.loc[run_metas["type"] == "STR", "value_str"] = "foo"
        run_metas.loc[run_metas["type"] == "BOOL", "value_bool"] = False
        run_metas.loc[run_metas["type"] == "FLOAT", "value_float"] = 0.00314
        run_metas = self.add_ids(run_metas)

        timeseries = self.datapoints[
            ["model", "scenario", "version", "region", "variable", "unit"]
        ].drop_duplicates()
        # join runs
        timeseries = timeseries.merge(
            runs,
            left_on=["model", "scenario", "version"],
            right_on=["model", "scenario", "version"],
            suffixes=("", "_run"),
        )
        timeseries = timeseries.rename(columns={"id": "run__id"})

        timeseries = timeseries.merge(
            measurands,
            left_on=["unit", "variable"],
            right_on=["unit", "variable"],
            suffixes=("", "_measurand"),
        )
        timeseries = timeseries.rename(columns={"id": "measurand__id"})

        measurands = measurands[["id", "unit__id", "variable__id"]]
        runs = runs[["id", "model__id", "scenario__id", "version"]]
        runs["is_default"] = False

        timeseries = self.merge_id_column(timeseries, regions, "region", "region__id")
        timeseries = self.add_ids(timeseries)
        datapoints = self.datapoints.merge(
            timeseries,
            left_on=[
                "model",
                "scenario",
                "version",
                "region",
                "unit",
                "variable",
            ],
            right_on=[
                "model",
                "scenario",
                "version",
                "region",
                "unit",
                "variable",
            ],
            suffixes=("", "_tseries"),
        )
        datapoints = datapoints.rename(columns={"id": "time_series__id"})
        datapoints = self.add_ids(datapoints)
        datapoints = datapoints[
            [
                "value",
                "type",
                "step_category",
                "step_year",
                "step_datetime",
                "time_series__id",
                "id",
            ]
        ]
        datapoints = datapoints.replace({np.nan: None})

        timeseries = timeseries[["region__id", "measurand__id", "run__id", "id"]]

        res = []
        for table_name, table_data in [
            ("model", models),
            ("scenario", scenarios),
            ("region", regions),
            ("unit", units),
            ("iamc_variable", variables),
            ("iamc_measurand", measurands),
            ("run", runs),
            ("runmetaentry", run_metas),
            ("iamc_timeseries", timeseries),
            ("iamc_datapoint_universal", datapoints),
        ]:
            table_data["__tablename__"] = table_name
            res += table_data.to_dict(orient="records")
        return res

    @classmethod
    def load_regions(cls, platform: ixmp4.Platform) -> None:
        for _, name, hierarchy in cls.regions.itertuples():
            platform.regions.create(name, hierarchy)

    @classmethod
    def load_units(cls, platform: ixmp4.Platform) -> None:
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_runs(cls, platform: ixmp4.Platform) -> None:
        for _, model, scenario, version, is_default in cls.runs.itertuples():
            run = platform.runs.create(model, scenario)
            if run.version != version:
                raise ProgrammingError("Run fixture incomplete or out of order.")

            if is_default:
                run.set_as_default()

    @classmethod
    def load_run_datapoints(
        cls, platform: ixmp4.Platform, run_tup: tuple[str, str, int], dps: pd.DataFrame
    ) -> None:
        run = platform.runs.get(*run_tup)

        annual = dps[dps["type"] == "ANNUAL"].dropna(how="all", axis="columns")
        categorical = dps[dps["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )
        datetime = dps[dps["type"] == "DATETIME"].dropna(how="all", axis="columns")
        with run.transact("Add iamc data"):
            if not annual.empty:
                run.iamc.add(annual, type=ixmp4.DataPoint.Type.ANNUAL)
            if not categorical.empty:
                run.iamc.add(categorical, type=ixmp4.DataPoint.Type.CATEGORICAL)
            if not datetime.empty:
                run.iamc.add(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    @classmethod
    def get_run_dps(
        cls, df: pd.DataFrame, model: str, scenario: str, version: int
    ) -> pd.DataFrame:
        dps = df.copy()
        dps = dps[dps["model"] == model]
        dps = dps[dps["scenario"] == scenario]
        dps = dps[dps["version"] == version]
        dps = dps.drop(columns=cls.run_cols)
        return dps

    @classmethod
    def load_dp_df(cls, platform: ixmp4.Platform, df: pd.DataFrame) -> None:
        runs = df[cls.run_cols].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            dps = cls.get_run_dps(df, model, scenario, version)
            cls.load_run_datapoints(platform, (model, scenario, version), dps)

    @classmethod
    def load_datapoints(cls, platform: ixmp4.Platform) -> None:
        cls.load_dp_df(platform, cls.datapoints)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform) -> None:
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
    def load_regions(cls, platform: ixmp4.Platform) -> None:
        for _, name, hierarchy in cls.regions.itertuples():
            platform.regions.create(name, hierarchy)

    @classmethod
    def load_units(cls, platform: ixmp4.Platform) -> None:
        for _, name in cls.units.itertuples():
            platform.units.create(name)

    @classmethod
    def load_runs(cls, platform: ixmp4.Platform) -> None:
        for _, model, scenario, version, is_default in cls.runs.itertuples():
            run = platform.runs.create(model, scenario)
            if run.version != version:
                raise ProgrammingError("Run fixture incomplete or out of order.")

            if is_default:
                run.set_as_default()

    @classmethod
    def load_dp_df(cls, platform: ixmp4.Platform, df: pd.DataFrame) -> None:
        runs = df[cls.run_cols].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            dps = cls.get_run_dps(df, model, scenario, version)
            cls.load_run_datapoints(platform, (model, scenario, version), dps)

    @classmethod
    def get_run_dps(
        cls, df: pd.DataFrame, model: str, scenario: str, version: int
    ) -> pd.DataFrame:
        dps = df.copy()
        dps = dps[dps["model"] == model]
        dps = dps[dps["scenario"] == scenario]
        dps = dps[dps["version"] == version]
        dps = dps.drop(columns=cls.run_cols)
        return dps

    @classmethod
    def rm_dp_df(cls, platform: ixmp4.Platform, df: pd.DataFrame) -> None:
        runs = df[cls.run_cols].copy()
        runs.drop_duplicates(inplace=True)
        for _, model, scenario, version in runs.itertuples():
            dps = cls.get_run_dps(df, model, scenario, version)
            cls.rm_run_datapoints(platform, (model, scenario, version), dps)

    @classmethod
    def load_datapoints(cls, platform: ixmp4.Platform) -> None:
        cls.load_dp_df(platform, cls.datapoints)

    @classmethod
    def load_datapoints_half(cls, platform: ixmp4.Platform) -> None:
        scrambled_dps = cls.datapoints.sample(frac=1)
        half_dps = scrambled_dps.head(len(scrambled_dps) // 2)
        half_dps = half_dps.sort_values(by=cls.run_cols)
        cls.load_dp_df(platform, half_dps)

    @classmethod
    def load_run_datapoints(
        cls, platform: ixmp4.Platform, run_tup: tuple[str, str, int], dps: pd.DataFrame
    ) -> None:
        run = platform.runs.get(*run_tup)

        with run.transact("Add iamc data"):
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
    ) -> None:
        run = platform.runs.get(*run_tup)

        annual = dps[dps["type"] == "ANNUAL"].dropna(how="all", axis="columns")
        categorical = dps[dps["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )
        datetime = dps[dps["type"] == "DATETIME"].dropna(how="all", axis="columns")
        with run.transact("Remove iamc data"):
            run.iamc.remove(annual, type=ixmp4.DataPoint.Type.ANNUAL)
            run.iamc.remove(categorical, type=ixmp4.DataPoint.Type.CATEGORICAL)
            run.iamc.remove(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    @classmethod
    def load_dataset(cls, platform: ixmp4.Platform) -> None:
        cls.load_regions(platform)
        cls.load_units(platform)
        cls.load_runs(platform)
        cls.load_datapoints(platform)
