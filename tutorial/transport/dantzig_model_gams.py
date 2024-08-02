import copy
from pathlib import Path

import gams.transfer as gt
import pandas as pd
from gams import GamsWorkspace

from ixmp4.core import Run


def write_run_to_gdx(run: Run, write_to: Path | str = "transport_data.gdx") -> None:
    """Writes scenario data from the Run to a GDX file."""
    write_to = Path(write_to).absolute()
    m = gt.Container()
    indexsets = [
        gt.Set(
            container=m,
            name=indexset.name,
            records=indexset.elements,
            description=indexset.docs
            if indexset.docs
            else "",  # description is "optional", but must be str
        )
        for indexset in run.optimization.indexsets.list()
    ]

    for scalar in run.optimization.scalars.list():
        gt.Parameter(
            container=m,
            name=scalar.name,
            records=scalar.value,
            description=scalar.docs if scalar.docs else "",
        )

    for parameter in run.optimization.parameters.list():
        domains = [
            indexset
            for indexset in indexsets
            if indexset.name in parameter.constrained_to_indexsets
        ]
        records = copy.deepcopy(parameter.data)
        del records[
            "units"
        ]  # all parameters must have units, but GAMS doesn't work on them
        gt.Parameter(
            container=m,
            name=parameter.name,
            domain=domains,
            records=records,
            description=parameter.docs if parameter.docs else "",
        )

    m.write(write_to=write_to)


def solve_dantzig_model(data_file: Path | str = "transport_data.gdx") -> None:
    ws = GamsWorkspace(working_directory=Path().absolute())
    # Data from previous step might be saved elsewhere
    ws.add_database_from_gdx(data_file)
    job = ws.add_job_from_file("transport_ixmp4.gms")
    job.run()


def read_dantzig_solution(run: Run) -> None:
    m = gt.Container("transport_results.gdx")
    for variable in run.optimization.variables.list():
        # DF also includes lower, upper, scale
        variable_data: pd.DataFrame = (
            m.data[variable.name]
            .records[["level", "marginal"]]
            .rename(columns={"level": "levels", "marginal": "marginals"})
        )
        run.optimization.variables.get(variable.name).add(data=variable_data)

    for equation in run.optimization.equations.list():
        equation_data: pd.DataFrame = (
            m.data[equation.name]
            .records[["level", "marginal"]]
            .rename(columns={"level": "levels", "marginal": "marginals"})
        )
        run.optimization.equations.get(equation.name).add(data=equation_data)
