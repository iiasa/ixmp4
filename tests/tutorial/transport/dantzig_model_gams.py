import copy
from pathlib import Path

import gams.transfer as gt
import pandas as pd
from gams import GamsWorkspace

from ixmp4.core import Run


def write_run_to_gams(run: Run) -> gt.Container:
    """Writes scenario data from the Run to a GAMS container."""
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

    return m


# TODO I'd like to have proper Paths here, but gams can only handle str
def solve(model_file: Path, data_file: Path, result_file: Path | None = None) -> None:
    ws = GamsWorkspace(working_directory=Path(__file__).parent.absolute())
    ws.add_database_from_gdx(gdx_file_name=str(data_file))
    gams_options = ws.add_options()
    gams_options.defines["in"] = str(data_file)
    if result_file:
        gams_options.defines["out"] = str(result_file)
    job = ws.add_job_from_file(file_name=str(model_file))
    job.run(gams_options=gams_options)


def read_solution_to_run(run: Run, result_file: Path) -> None:
    m = gt.Container(load_from=result_file)
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
