from typing import ClassVar

from ixmp4.data.db.optimization.parameter.model import ParameterIndexsetAssociation
from ixmp4.db import Session, filters, sql

from .. import Parameter, Run


class OptimizationParameterFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = Parameter

    def join(
        self, exc: sql.Select[tuple[Parameter]], session: Session | None = None
    ) -> sql.Select[tuple[Parameter]]:
        exc = exc.join(Run, onclause=Parameter.run__id == Run.id)
        return exc


class OptimizationParameterIndexSetAssociationFilter(
    filters.BaseFilter, metaclass=filters.FilterMeta
):
    id: filters.Id | None = filters.Field(None)
    parameter__id: filters.Integer | None = filters.Field(None, alias="parameter_id")

    sqla_model: ClassVar[type] = ParameterIndexsetAssociation
