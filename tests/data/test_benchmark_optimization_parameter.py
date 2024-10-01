"""These tests just benchmark pandas and in-DB-json editing for comparison."""

import ixmp4

from ..fixtures import BigOptimizationDataset


class TestOptimizationParameter:
    data = BigOptimizationDataset()
    parameter_id: int

    def test_add_data(self, platform: ixmp4.Platform, profiled, benchmark):
        """Benchmarks add_data using a large parameter."""

        def setup():
            self.data.load_units(platform)
            self.data.load_indexsets(platform)
            self.parameter_id = self.data.load_parameter(platform)
            return (platform,), {}

        def run(mp):
            with profiled():
                self.data.insert_parameter_data(mp, self.parameter_id)
                self.data.upsert_parameter_data(mp, self.parameter_id)

        benchmark.pedantic(run, setup=setup)

    def test_add_data_json(self, platform: ixmp4.Platform, profiled, benchmark):
        """Benchmarks add_data_json using a large parameter."""

        def setup():
            self.data.load_units(platform)
            self.data.load_indexsets(platform)
            self.parameter_id = self.data.load_parameter(platform)
            return (platform,), {}

        def run(mp):
            with profiled():
                self.data.insert_parameter_data_json(mp, self.parameter_id)
                self.data.upsert_parameter_data_json(mp, self.parameter_id)

        benchmark.pedantic(run, setup=setup)
