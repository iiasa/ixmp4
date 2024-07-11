import linopy
import pandas as pd

from ixmp4.core import Equation, IndexSet, Parameter, Scalar
from ixmp4.core import OptimizationVariable as Variable


def create_set(indexset: IndexSet, name: str | None = None) -> pd.Index:
    return pd.Index(indexset.elements, name=name or indexset.name)


def create_parameter(
    parameter: Parameter, index: pd.Index | list[pd.Index], name: str
) -> pd.Series:
    if isinstance(index, list):
        index = pd.MultiIndex.from_product(index)

    return pd.Series(data=parameter.values, index=index, name=name)


def create_dantzig_model(
    i: IndexSet,
    j: IndexSet,
    a: Parameter,
    b: Parameter,
    d: Parameter,
    f: Scalar,
) -> linopy.Model:
    m = linopy.Model()
    i_set = create_set(indexset=i, name="Canning Plants")
    j_set = create_set(indexset=j, name="Markets")
    a_parameter = create_parameter(
        parameter=a, index=i_set, name="capacity of plant i in cases"
    )
    b_parameter = create_parameter(
        parameter=b, index=j_set, name="demand at market j in cases"
    )
    d_parameter = create_parameter(
        parameter=d, index=[i_set, j_set], name="distance in thousands of miles"
    )
    f_scalar = f.value

    c = d_parameter * f_scalar / 1000
    c.name = "transport cost in thousands of dollars per case"

    x = m.add_variables(
        lower=0.0, coords=[i_set, j_set], name="Shipment quantities in cases"
    )

    # The constraints don't seem to be typed correctly by linopy
    con = x.sum(dim="Markets") <= a_parameter
    m.add_constraints(con, name="Observe supply limit at plant i")  # type: ignore

    con = x.sum(dim="Canning Plants") >= b_parameter
    m.add_constraints(con, name="Satisfy demand at market j")  # type: ignore

    obj = c.to_xarray() * x
    m.add_objective(obj)

    return m


def read_dantzig_solution(
    model: linopy.Model, z: Variable, x: Variable, demand: Equation, supply: Equation
) -> None:
    # Handle objective
    # TODO adding fake marginals here until Variables don't require this column anymore
    # Can't add units if this column was not declared above. Better stored as Scalar
    # maybe?
    z.add(data={"levels": [model.objective.value], "marginals": [-0.0]})

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
    x.add(data=x_data)

    # The following don't seem to be typed correctly by linopy
    # Add supply data
    supply_data = {
        "i": ["seattle", "san-diego"],
        "levels": model.constraints["Observe supply limit at plant i"].data.rhs,  # type: ignore
        "marginals": model.constraints["Observe supply limit at plant i"].data.dual,  # type: ignore
    }
    supply.add(data=supply_data)

    # Add demand data
    demand_data = {
        "j": ["new-york", "chicago", "topeka"],
        "levels": model.constraints["Satisfy demand at market j"].data.rhs,  # type: ignore
        "marginals": model.constraints["Satisfy demand at market j"].data.dual,  # type: ignore
    }
    demand.add(data=demand_data)
