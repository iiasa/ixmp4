from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

from sqlalchemy import sql

from ixmp4.data.db.versions.model import DefaultVersionModel

if TYPE_CHECKING:
    from ixmp4.data.db.optimization.equation.model import Equation, EquationVersion
    from ixmp4.data.db.optimization.indexset.model import IndexSet, IndexSetVersion
    from ixmp4.data.db.optimization.parameter.model import Parameter, ParameterVersion
    from ixmp4.data.db.optimization.scalar.model import Scalar, ScalarVersion
    from ixmp4.data.db.optimization.table.model import Table, TableVersion
    from ixmp4.data.db.optimization.variable.model import (
        OptimizationVariable,
        VariableVersion,
    )
    from ixmp4.data.db.unit import Unit, UnitVersion


def where_recorded_after_transaction(
    exc: sql.Select[Any], transaction__id: int, model_class: type[DefaultVersionModel]
) -> sql.Select[Any]:
    return exc.where(model_class.transaction_id > transaction__id)


def where_valid_at_transaction(
    exc: sql.Select[Any], transaction__id: int, model_class: type[DefaultVersionModel]
) -> sql.Select[Any]:
    return exc.where(
        sql.and_(
            model_class.transaction_id <= transaction__id,
            model_class.operation_type != 2,  # versions.model.Operation.DELETE
            sql.or_(
                model_class.end_transaction_id > transaction__id,
                model_class.end_transaction_id == sql.null(),
            ),
        )
    )


def apply_transaction__id(
    exc: sql.Select[Any],
    model_class: type[DefaultVersionModel],
    transaction__id: int | None,
    valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
) -> sql.Select[Any]:
    _exc = exc

    if transaction__id is not None:
        match valid:
            case "at_transaction":
                _exc = where_valid_at_transaction(_exc, transaction__id, model_class)
            case "after_transaction":
                _exc = where_recorded_after_transaction(
                    _exc, transaction__id, model_class
                )

    return _exc


# NOTE TypeVar doesn't like variables (e.g. sets that expand to these lists, so not
# sure how to avoid this repetition)
NamedModelType = TypeVar("NamedModelType", type["Unit"], type["UnitVersion"])
RunLinkedModelType = TypeVar(
    "RunLinkedModelType",
    type["Equation"],
    type["EquationVersion"],
    type["IndexSet"],
    type["IndexSetVersion"],
    type["Parameter"],
    type["ParameterVersion"],
    type["Scalar"],
    type["ScalarVersion"],
    type["Table"],
    type["TableVersion"],
    type["OptimizationVariable"],
    type["VariableVersion"],
)
SelectModelType = TypeVar(
    "SelectModelType",
    type["Unit"],
    type["UnitVersion"],
    type["Equation"],
    type["EquationVersion"],
    type["IndexSet"],
    type["IndexSetVersion"],
    type["Parameter"],
    type["ParameterVersion"],
    type["Scalar"],
    type["ScalarVersion"],
    type["Table"],
    type["TableVersion"],
    type["OptimizationVariable"],
    type["VariableVersion"],
)


@overload
def select_for_id_map(
    model_class: NamedModelType, run__id: None, transaction__id: int | None = None
) -> sql.Select[tuple[int, str]]: ...


@overload
def select_for_id_map(
    model_class: RunLinkedModelType, run__id: int, transaction__id: int | None = None
) -> sql.Select[tuple[int, str]]: ...


def select_for_id_map(
    model_class: SelectModelType,
    run__id: int | None,
    transaction__id: int | None = None,
) -> sql.Select[tuple[int, str]]:
    exc: sql.Select[tuple[int, str]] = sql.Select(model_class.id, model_class.name)

    if run__id is not None:
        # NOTE I think this is on mypy to not recognize that run__id can only be not
        # None when called with a model_class that has run__id
        exc = exc.where(model_class.run__id == run__id)  # type: ignore[attr-defined]

    if issubclass(model_class, DefaultVersionModel):
        exc = apply_transaction__id(
            exc=exc, transaction__id=transaction__id, model_class=model_class
        )

    return exc
