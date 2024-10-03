import numpy as np
import pandas as pd

import ixmp4
from ixmp4.core import Run, Unit

from .dantzig_model_linopy import (
    create_dantzig_model,
    read_dantzig_solution,
)


def create_dantzig_run(mp: ixmp4.Platform) -> Run:
    """Create a Run for the transport tutorial.

    Please see the tutorial file for explanation.
    """
    # Only needed once for each mp
    try:
        cases = mp.units.get("cases")
        km = mp.units.get("km")
        unit_cost_per_case = mp.units.get("USD/km")
    except Unit.NotFound:
        cases = mp.units.create("cases")
        km = mp.units.create("km")
        unit_cost_per_case = mp.units.create("USD/km")

    # Create run and all data sets
    run = mp.runs.create(model="transport problem", scenario="standard")
    a_data = {
        "i": ["seattle", "san-diego"],
        "values": [350, 600],
        "units": [cases.name, cases.name],
    }
    b_data = pd.DataFrame(
        [
            ["new-york", 325, cases.name],
            ["chicago", 300, cases.name],
            ["topeka", 275, cases.name],
        ],
        columns=["j", "values", "units"],
    )
    d_data = {
        "i": ["seattle", "seattle", "seattle", "san-diego", "san-diego", "san-diego"],
        "j": ["new-york", "chicago", "topeka", "new-york", "chicago", "topeka"],
        "values": [2.5, 1.7, 1.8, 2.5, 1.8, 1.4],
        "units": [km.name] * 6,
    }

    # Add all data to the run
    run.optimization.indexsets.create("i").add(["seattle", "san-diego"])
    run.optimization.indexsets.create("j").add(["new-york", "chicago", "topeka"])
    run.optimization.parameters.create(name="a", constrained_to_indexsets=["i"]).add(
        data=a_data
    )
    run.optimization.parameters.create("b", constrained_to_indexsets=["j"]).add(
        data=b_data
    )
    run.optimization.parameters.create("d", constrained_to_indexsets=["i", "j"]).add(
        data=d_data
    )
    run.optimization.scalars.create(name="f", value=90, unit=unit_cost_per_case)

    # Create further optimization items to store solution data
    run.optimization.variables.create("z")
    run.optimization.variables.create("x", constrained_to_indexsets=["i", "j"])
    run.optimization.equations.create("supply", constrained_to_indexsets=["i"])
    run.optimization.equations.create("demand", constrained_to_indexsets=["j"])

    return run


class TestTransportTutorialLinopy:
    def test_create_dantzig_model(self, platform: ixmp4.Platform):
        run = create_dantzig_run(platform)
        model = create_dantzig_model(run)

        # Set expectations
        expected: dict[str, pd.Series] = {
            "supply_constraint_sign": pd.Series(["<=", "<="]),
            "demand_constraint_sign": pd.Series([">=", ">=", ">="]),
            "supply_constraint_rhs": pd.Series([350.0, 600.0]),
            "demand_constraint_rhs": pd.Series([325.0, 300.0, 275.0]),
            "objective_coeffs": pd.Series([0.162, 0.225, 0.126, 0.153, 0.225, 0.162]),
        }

        assert model.variables["Shipment quantities in cases"].dims == (
            "Canning Plants",
            "Markets",
        )
        assert model.constraints["Observe supply limit at plant i"].coord_dims == (
            "Canning Plants",
        )
        assert np.allclose(
            model.constraints["Observe supply limit at plant i"].data.rhs.values,
            expected["supply_constraint_rhs"],
        )
        # TODO Replace this with np.strings.equal once supporting numpy >= 2.0.0
        assert np.char.equal(
            model.constraints["Observe supply limit at plant i"].data.sign.values,
            expected["supply_constraint_sign"],
        ).all()
        assert model.constraints["Satisfy demand at market j"].coord_dims == (
            "Markets",
        )
        assert np.allclose(
            model.constraints["Satisfy demand at market j"].data.rhs.values,
            expected["demand_constraint_rhs"],
        )
        # TODO Replace this with np.strings.equal once supporting numpy >= 2.0.0
        assert np.char.equal(
            model.constraints["Satisfy demand at market j"].data.sign.values,
            expected["demand_constraint_sign"],
        ).all()
        assert model.objective.sense == "min"

        assert np.allclose(
            model.objective.coeffs.to_pandas(), expected["objective_coeffs"]
        )

    def test_read_dantzig_solution(self, platform: ixmp4.Platform):
        # Could we store this as class attributes to avoid repetition?
        run = create_dantzig_run(platform)
        model = create_dantzig_model(run)
        model.solve("highs")
        read_dantzig_solution(model=model, run=run)

        # Assert what we want to show in the tutorial
        assert run.optimization.variables.get("z").levels == [153.675]
        assert run.optimization.variables.get("x").data == {
            "i": [
                "seattle",
                "seattle",
                "seattle",
                "san-diego",
                "san-diego",
                "san-diego",
            ],
            "j": ["new-york", "chicago", "topeka", "new-york", "chicago", "topeka"],
            "levels": [0.0, 300.0, 0.0, 325.0, 0.0, 275.0],
            "marginals": [-0.0, -0.0, -0.0, -0.0, -0.0, -0.0],
        }
        assert run.optimization.equations.get("demand").data == {
            "j": ["new-york", "chicago", "topeka"],
            "levels": [325.0, 300.0, 275.0],
            "marginals": [0.225, 0.153, 0.126],
        }
        assert run.optimization.equations.get("supply").data == {
            "i": ["seattle", "san-diego"],
            "levels": [350.0, 600.0],
            "marginals": [-0.0, -0.0],
        }
