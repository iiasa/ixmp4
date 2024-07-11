import linopy
import pandas as pd

from ixmp4.core import Parameter, Run


def create_parameter(
    parameter: Parameter, index: pd.Index | list[pd.Index], name: str
) -> pd.Series:
    if isinstance(index, list):
        index = pd.MultiIndex.from_product(index)

    return pd.Series(data=parameter.values, index=index, name=name)


def create_dantzig_model(run: Run) -> linopy.Model:
    m = linopy.Model()
    i = pd.Index(run.optimization.indexsets.get("i").elements, name="Canning Plants")
    j = pd.Index(run.optimization.indexsets.get("j").elements, name="Markets")
    a = create_parameter(
        parameter=run.optimization.parameters.get("a"),
        index=i,
        name="capacity of plant i in cases",
    )
    b = create_parameter(
        parameter=run.optimization.parameters.get("b"),
        index=j,
        name="demand at market j in cases",
    )
    d = create_parameter(
        parameter=run.optimization.parameters.get("d"),
        index=[i, j],
        name="distance in thousands of miles",
    )
    f = run.optimization.scalars.get("f").value

    c = d * f / 1000
    c.name = "transport cost in thousands of dollars per case"

    x = m.add_variables(lower=0.0, coords=[i, j], name="Shipment quantities in cases")

    m.add_constraints(
        lhs=x.sum(dim="Markets"),
        sign="<=",
        rhs=a,
        name="Observe supply limit at plant i",
    )

    m.add_constraints(
        lhs=x.sum(dim="Canning Plants"),
        sign=">=",
        rhs=b,
        name="Satisfy demand at market j",
    )

    obj = c.to_xarray() * x
    m.add_objective(obj)

    return m


def read_dantzig_solution(model: linopy.Model, run: Run) -> None:
    # Handle objective
    # TODO adding fake marginals here until Variables don't require this column anymore
    # Can't add units if this column was not declared above. Better stored as Scalar
    # maybe?
    run.optimization.variables.get("z").add(
        data={"levels": [model.objective.value], "marginals": [-0.0]}
    )

    # Handle shipment quantities
    x_data: pd.DataFrame = model.solution.to_dataframe()
    x_data.reset_index(inplace=True)
    x_data.rename(
        columns={
            "Shipment quantities in cases": "levels",
            "Canning Plants": "i",
            "Markets": "j",
        },
        inplace=True,
    )
    # x_data["units"] = "cases"
    # TODO Again setting fake marginals until they are optional for variables
    x_data["marginals"] = -0.0
    run.optimization.variables.get("x").add(data=x_data)

    # The following don't seem to be typed correctly by linopy
    # Add supply data
    supply_data = {
        "i": ["seattle", "san-diego"],
        "levels": model.constraints["Observe supply limit at plant i"].data.rhs,  # type: ignore
        "marginals": model.constraints["Observe supply limit at plant i"].data.dual,  # type: ignore
    }
    run.optimization.equations.get("supply").add(data=supply_data)

    # Add demand data
    demand_data = {
        "j": ["new-york", "chicago", "topeka"],
        "levels": model.constraints["Satisfy demand at market j"].data.rhs,  # type: ignore
        "marginals": model.constraints["Satisfy demand at market j"].data.dual,  # type: ignore
    }
    run.optimization.equations.get("demand").add(data=demand_data)
