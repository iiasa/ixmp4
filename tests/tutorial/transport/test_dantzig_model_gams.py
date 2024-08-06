import copy
import shutil
from pathlib import Path

from gams.transfer import Set

from ixmp4 import Platform

from ...utils import all_platforms, create_dantzig_run
from .dantzig_model_gams import read_solution_to_run, solve, write_run_to_gams


@all_platforms
class TestTransportTutorialLinopy:
    # NOTE The function could be expanded by tables, equations, variables, none of which
    #  are tested here.
    def test_write_run_to_gams(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = create_dantzig_run(test_mp)
        gams_container = write_run_to_gams(run=run)

        # Should include exactly i, j, f, a, b, d
        assert len(gams_container.data.keys()) == 6

        gams_indexsets: list[Set] = []  # type: ignore
        for indexset in run.optimization.indexsets.list():
            gams_indexset = gams_container.data[indexset.name]
            assert gams_indexset.name == indexset.name
            assert gams_indexset.records["uni"].to_list() == indexset.elements
            gams_indexsets.append(gams_indexset)

        for scalar in run.optimization.scalars.list():
            gams_scalar = gams_container.data[scalar.name]
            assert gams_scalar.name == scalar.name
            # Should only have one value
            assert len(gams_scalar.records["value"]) == 1
            assert gams_scalar.records["value"].values[0] == scalar.value

        for parameter in run.optimization.parameters.list():
            gams_parameter = gams_container.data[parameter.name]
            assert gams_parameter.name == parameter.name

            expected_domains = [
                indexset
                for indexset in gams_indexsets
                if indexset.name in parameter.constrained_to_indexsets
            ]
            assert gams_parameter.domain == expected_domains

            expected_records = copy.deepcopy(parameter.data)
            del expected_records[
                "units"
            ]  # all parameters must have units, but GAMS doesn't work on them
            assert (
                gams_parameter.records.rename(columns={"value": "values"}).to_dict(
                    orient="list"
                )
                == expected_records
            )

    def test_solve(self, test_mp, request, tmp_path):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = create_dantzig_run(test_mp)
        gams_container = write_run_to_gams(run=run)
        data_file = tmp_path / "transport_data.gdx"
        # TODO once we know where the tests land, figure out how to navigate paths
        # Same below.
        model_file = shutil.copy(
            src=Path(__file__).parent.absolute() / "transport_ixmp4.gms",
            dst=tmp_path / "transport_ixmp4.gms",
        )
        gams_container.write(write_to=data_file)

        # Test writing to default location
        solve(model_file=model_file, data_file=data_file)
        default_result_file = Path(__file__).parent.absolute() / "transport_results.gdx"
        assert default_result_file.is_file()

        # Test writing to specified location
        result_file: Path = tmp_path / "different_transport_results.gdx"  # type: ignore
        solve(model_file=model_file, data_file=data_file, result_file=result_file)
        assert result_file.is_file()

    # TODO Maybe this test could be made more performant by receiving a run where the
    # scenario has already been solved. However, this would make the test scenario less
    # isolated. Also, solving the dantzig model only takes a few seconds.
    def test_read_solution_to_run(self, test_mp, request, tmp_path):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = create_dantzig_run(test_mp)
        gams_container = write_run_to_gams(run=run)
        data_file = tmp_path / "transport_data.gdx"
        model_file = shutil.copy(
            src=Path(__file__).parent.absolute() / "transport_ixmp4.gms",
            dst=tmp_path / "transport_ixmp4.gms",
        )
        gams_container.write(write_to=data_file)
        solve(model_file=model_file, data_file=data_file)
        read_solution_to_run(
            run=run,
            result_file=Path(__file__).parent.absolute() / "transport_results.gdx",
        )

        # Test objective value
        z = run.optimization.variables.get("z")
        assert z.levels == [153.675]
        assert z.marginals == [0.0]

        # Test shipment quantities
        assert run.optimization.variables.get("x").data == {
            "levels": [50.0, 300.0, 0.0, 275.0, 0.0, 275.0],
            "marginals": [
                0.0,
                0.0,
                0.036000000000000004,
                0.0,
                0.009000000000000008,
                0.0,
            ],
        }

        # Test demand equation
        assert run.optimization.equations.get("demand").data == {
            "levels": [325.0, 300.0, 275.0],
            "marginals": [0.225, 0.153, 0.126],
        }

        # Test supply equation
        assert run.optimization.equations.get("supply").data == {
            "levels": [350.0, 550.0],
            "marginals": [-0.0, 0.0],
        }
