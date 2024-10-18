import pandas as pd

import ixmp4
from ixmp4.core import Run, Unit


def create_default_dantzig_run(platform: ixmp4.Platform) -> Run:
    """Creates new ixmp4.Run holding all data for Dantzig's problem."""
    try:
        cases = platform.units.get("cases")
        km = platform.units.get("km")
        unit_cost_per_case = platform.units.get("USD/km")
    except Unit.NotFound:
        cases = platform.units.create("cases")
        km = platform.units.create("km")
        unit_cost_per_case = platform.units.create("USD/km")
    run = platform.runs.create(model="transport problem", scenario="standard")
    run.set_as_default()
    run.optimization.indexsets.create("i").add(["seattle", "san-diego"])
    run.optimization.indexsets.create("j").add(["new-york", "chicago", "topeka"])
    a_data = {
        "i": ["seattle", "san-diego"],
        "values": [350, 600],
        "units": [cases.name, cases.name],
    }
    run.optimization.parameters.create(name="a", constrained_to_indexsets=["i"]).add(
        data=a_data
    )
    b_data = pd.DataFrame(
        [
            ["new-york", 325, cases.name],
            ["chicago", 300, cases.name],
            ["topeka", 275, cases.name],
        ],
        columns=["j", "values", "units"],
    )
    run.optimization.parameters.create("b", constrained_to_indexsets=["j"]).add(b_data)
    d_data = {
        "i": ["seattle", "seattle", "seattle", "san-diego", "san-diego", "san-diego"],
        "j": ["new-york", "chicago", "topeka", "new-york", "chicago", "topeka"],
        "values": [2.5, 1.7, 1.8, 2.5, 1.8, 1.4],
        "units": [km.name] * 6,
    }
    run.optimization.parameters.create("d", constrained_to_indexsets=["i", "j"]).add(
        d_data
    )
    run.optimization.scalars.create(name="f", value=90, unit=unit_cost_per_case)
    run.optimization.variables.create("z")
    run.optimization.variables.create("x", constrained_to_indexsets=["i", "j"])
    run.optimization.equations.create("supply", constrained_to_indexsets=["i"])
    run.optimization.equations.create("demand", constrained_to_indexsets=["j"])

    return run
