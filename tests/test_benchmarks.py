"""This module only contains benchmarks, no assertions are made to validate the
results."""

import ixmp4

from .fixtures import BigIamcDataset


class TestBenchmarks:
    big = BigIamcDataset()

    def test_add_datapoints_full_benchmark(
        self, platform: ixmp4.Platform, profiled, benchmark
    ):
        """Benchmarks a full insert of `test_data_big`."""

        def setup():
            self.big.load_regions(platform)
            self.big.load_units(platform)
            self.big.load_runs(platform)
            return (platform,), {}

        def run(mp):
            with profiled():
                self.big.load_datapoints(mp)

        benchmark.pedantic(run, setup=setup)

    def test_add_datapoints_half_unchanged_benchmark(
        self, platform: ixmp4.Platform, profiled, benchmark
    ):
        """Benchmarks a full insert of `test_data_big` on a half-filled database."""

        def setup():
            self.big.load_regions(platform)
            self.big.load_units(platform)
            self.big.load_runs(platform)
            self.big.load_datapoints_half(platform)
            return (platform,), {}

        def run(mp):
            with profiled():
                self.big.load_datapoints(mp)

        benchmark.pedantic(run, setup=setup)

    def test_add_datapoints_half_insert_half_update_benchmark(
        self, platform: ixmp4.Platform, profiled, benchmark
    ):
        """Benchmarks a full insert of `test_data_big` with changed values on a
        half-filled database."""

        def setup():
            self.big.load_regions(platform)
            self.big.load_units(platform)
            self.big.load_runs(platform)
            self.big.load_datapoints_half(platform)
            datapoints = self.big.datapoints.copy()
            datapoints["value"] = -9999
            return (platform, datapoints), {}

        def run(mp, data):
            with profiled():
                self.big.load_dp_df(mp, data)

        benchmark.pedantic(run, setup=setup)

        ret = platform.iamc.tabulate(run={"default_only": False})
        assert ret["value"].unique() == [-9999]

    def test_remove_datapoints_benchmark(
        self, platform: ixmp4.Platform, profiled, benchmark
    ):
        """Benchmarks a full removal of `test_data_big` from a filled database."""

        def setup():
            self.big.load_regions(platform)
            self.big.load_units(platform)
            self.big.load_runs(platform)
            self.big.load_datapoints(platform)
            data = self.big.datapoints.copy().drop(columns=["value"])
            return (platform, data), {}

        def run(mp, data):
            with profiled():
                self.big.rm_dp_df(mp, data)

        benchmark.pedantic(run, setup=setup)
        ret = platform.iamc.tabulate(run={"default_only": False})
        assert ret.empty

    def test_tabulate_datapoints_benchmark(
        self, platform: ixmp4.Platform, profiled, benchmark
    ):
        """Benchmarks a full retrieval of `test_data_big` from a filled database."""

        def setup():
            self.big.load_regions(platform)
            self.big.load_units(platform)
            self.big.load_runs(platform)
            self.big.load_datapoints(platform)
            return (platform,), {}

        def run(mp):
            with profiled():
                mp.iamc.tabulate(run={"default_only": False})

        benchmark.pedantic(run, setup=setup)
