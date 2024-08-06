import numpy as np
import pandas as pd
import xarray as xr

from ixmp4 import Platform

from ...utils import all_platforms, create_dantzig_run
from .dantzig_model_linopy import (
    create_dantzig_model,
    read_dantzig_solution,
)


@all_platforms
class TestTransportTutorialLinopy:
    def test_create_dantzig_model(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = create_dantzig_run(test_mp)
        model = create_dantzig_model(run)

        # Set expectations
        expected = {
            "supply_constraint_sign": xr.DataArray(["<=", "<="]),
            "demand_constraint_sign": xr.DataArray([">=", ">=", ">="]),
            # TODO enable this once #95 is merged; allows removal of xarray from file
            # "supply_constraint_sign": np.array(["<=", "<="]),
            # "demand_constraint_sign": np.array([">=", ">=", ">="]),
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
        assert (
            model.constraints["Observe supply limit at plant i"].data.sign.values
            == expected["supply_constraint_sign"]
        ).all()
        # TODO enable this once #95 is merged
        # assert np.strings.equal(
        #     model.constraints["Observe supply limit at plant i"].data.sign.values,
        #     expected["supply_constraint_sign"],
        # ).all()
        assert model.constraints["Satisfy demand at market j"].coord_dims == (
            "Markets",
        )
        assert np.allclose(
            model.constraints["Satisfy demand at market j"].data.rhs.values,
            expected["demand_constraint_rhs"],
        )
        assert (
            model.constraints["Satisfy demand at market j"].data.sign.values
            == expected["demand_constraint_sign"]
        ).all()
        # TODO enable this once #95 is merged
        # assert np.strings.equal(
        #     model.constraints["Satisfy demand at market j"].data.sign.values,
        #     expected["demand_constraint_sign"],
        # ).all()
        assert model.objective.sense == "min"

        assert np.allclose(
            model.objective.coeffs.to_pandas(), expected["objective_coeffs"]
        )

    def test_read_dantzig_solution(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore

        # Could we store this as class attributes to avoid repetition?
        run = create_dantzig_run(test_mp)
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
