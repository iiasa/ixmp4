import operator
from collections.abc import Callable, Iterable
from types import GenericAlias, UnionType
from typing import Any, ClassVar, Optional, TypeVar, Union, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic.fields import FieldInfo

# TODO Import this from typing when dropping support for 3.10
from typing_extensions import Self

from ixmp4 import db
from ixmp4.core.exceptions import BadFilterArguments, ProgrammingError

in_Type = TypeVar("in_Type")


def in_(
    c: db.typing_column[in_Type], v: Iterable[in_Type] | db.BindParameter[in_Type]
) -> db.BinaryExpression[bool]:
    return c.in_(v)


def like(c: db.typing_column[str], v: str) -> db.BinaryExpression[bool]:
    return c.like(escape_wildcard(v), escape="\\")


def ilike(c: db.typing_column[str], v: str) -> db.BinaryExpression[bool]:
    return c.ilike(escape_wildcard(v), escape="\\")


def notlike(c: db.typing_column[str], v: str) -> db.BinaryExpression[bool]:
    return c.notlike(escape_wildcard(v), escape="\\")


def notilike(c: db.typing_column[str], v: str) -> db.BinaryExpression[bool]:
    return c.notilike(escape_wildcard(v), escape="\\")


def escape_wildcard(v: str) -> str:
    return v.replace("%", "\\%").replace("*", "%")


class Integer(int):
    """An explicit proxy type for `int`."""

    pass


class Float(float):
    """An explicit proxy type for `float`."""

    pass


class Id(int):
    """A no-op type for a reduced set of `Integer` lookups."""

    pass


class String(str):
    """An explicit proxy type for `str`."""

    pass


Boolean = bool

argument_seperator = "__"
filter_func_prefix = "filter_"
lookup_map: dict[type, dict[str, tuple[type, Callable[..., Any]]]] = {
    Id: {
        "__root__": (int, operator.eq),
        "in": (list[int], in_),
    },
    Float: {
        "__root__": (int, operator.eq),
        "in": (list[int], in_),
        "gt": (int, operator.gt),
        "lt": (int, operator.lt),
        "gte": (int, operator.ge),
        "lte": (int, operator.le),
    },
    Integer: {
        "__root__": (int, operator.eq),
        "in": (list[int], in_),
        "gt": (int, operator.gt),
        "lt": (int, operator.lt),
        "gte": (int, operator.ge),
        "lte": (int, operator.le),
    },
    Boolean: {
        "__root__": (bool, operator.eq),
    },
    String: {
        "__root__": (str, operator.eq),
        "in": (list[str], in_),
        "like": (str, like),
        "ilike": (str, ilike),
        "notlike": (str, notlike),
        "notilike": (str, notilike),
    },
}


def get_filter_func_name(n: str) -> str:
    return filter_func_prefix + n.strip()


def _ensure_str_list(any_list: list[Any]) -> list[str]:
    str_list: list[str] = []
    for item in any_list:
        if isinstance(item, str):
            str_list.append(item)
        else:
            raise ProgrammingError("Field argument `lookups` must be `list` of `str`.")

    return str_list


PydanticMeta: type = type(BaseModel)


# NOTE mypy seems to say PydanticMeta has type Any, don't see how we could change that
class FilterMeta(PydanticMeta):  # type: ignore[misc]
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type["BaseFilter"]:
        annots = namespace.get("__annotations__", {}).copy()
        for name, annot in annots.items():
            if get_origin(annot) == ClassVar:
                continue
            cls.process_field(namespace, name, annot)

        return cast(FilterMeta, super().__new__(cls, name, bases, namespace, **kwargs))

    @classmethod
    def build_lookups(
        cls, field_type: type
    ) -> dict[str, tuple[type, Callable[..., Any]]]:
        global lookup_map
        if field_type not in lookup_map.keys():
            if get_origin(field_type) in [Union, UnionType]:
                unified_types: tuple[type, ...] = get_args(field_type)
                ut_lookup_map = {
                    type_: cls.build_lookups(type_) for type_ in unified_types
                }
                all_lookup_names: set[str] = set()
                for tl in ut_lookup_map.values():
                    all_lookup_names |= set(tl.keys())

                lookups: dict[str, tuple[type, Callable[..., Any]]] = {}
                for lookup_name in all_lookup_names:
                    tuples = [
                        ut_lookup_map[type_][lookup_name]
                        for type_ in unified_types
                        if ut_lookup_map[type_].get(lookup_name, None) is not None
                    ]
                    types, _ = zip(*tuples)

                    def lookup_func(
                        c: db.typing_column[Any],
                        v: Integer
                        | Float
                        | Id
                        | String
                        | Boolean
                        | list[Integer | Float | Id | String | Boolean],
                        tuples: list[tuple[type, Callable[..., Any]]] = tuples,
                    ) -> Any:
                        for t, lf in tuples:
                            # NOTE can't check isinstance(..., list[...]) directly, but
                            # all these cases call the same lf() anyway; skipping
                            if isinstance(t, GenericAlias):
                                return lf(c, v)
                            if isinstance(v, t):
                                return lf(c, v)
                        raise ProgrammingError

                    lookups[lookup_name] = (
                        # dynamic union types can't
                        # be done according to type checkers
                        Union[tuple(types)],  # type: ignore[assignment]
                        lookup_func,
                    )
                return lookups
            else:
                return {"__root__": (field_type, operator.eq)}
        else:
            return lookup_map[field_type]

    @classmethod
    def process_field(
        cls, namespace: dict[str, Any], field_name: str, field_type: type
    ) -> None:
        lookups = cls.build_lookups(field_type)
        field: FieldInfo | None = namespace.get(field_name, Field(default=None))

        if field is None:
            return

        namespace.setdefault(field_name, field)
        override_lookups: list[str] | None = None
        if isinstance(field.json_schema_extra, dict):
            jschema_lookups = field.json_schema_extra.get("lookups", None)
            # NOTE We can't `isinstance` parametrized generics. Nothing seems to utilize
            # `lookups`, though, so this should not worsen performance.
            if isinstance(jschema_lookups, list):
                override_lookups = _ensure_str_list(jschema_lookups)
        else:
            override_lookups = None
        if isinstance(override_lookups, list):
            lookups = {k: v for k, v in lookups.items() if k in override_lookups}
        else:  # override_lookups is None
            pass
        base_field_alias = str(field.alias) if field.alias else field_name

        cls.expand_lookups(
            field_name,
            lookups,
            namespace,
            base_field_alias,
        )

    @classmethod
    def expand_lookups(
        cls,
        name: str,
        lookups: dict[str, tuple[type, Callable[..., Any]]],
        namespace: dict[str, Any],
        base_field_alias: str | None = None,
    ) -> None:
        global argument_seperator
        for lookup_alias, (type_, func) in lookups.items():
            filter_name = (
                name
                if lookup_alias == "__root__"
                else name + argument_seperator + lookup_alias
            )

            namespace["__annotations__"][filter_name] = Optional[type_]
            func_name = get_filter_func_name(filter_name)

            FilterType = TypeVar("FilterType")

            def filter_func(
                self: Self,
                exc: db.sql.Select[tuple[FilterType, ...]],
                f: str,
                v: Integer | Float | Id | String | Boolean,
                func: Callable[
                    [str, Integer | Float | Id | String | Boolean],
                    db.ColumnExpressionArgument[bool],
                ] = func,
                session: db.Session | None = None,
            ) -> db.sql.Select[tuple[FilterType, ...]]:
                return exc.where(func(f, v))

            namespace.setdefault(func_name, filter_func)

            if (
                base_field_alias is not None
                and base_field_alias != name
                and lookup_alias != "__root__"
            ):
                field = namespace.get(
                    filter_name,
                    Field(
                        None, alias=base_field_alias + argument_seperator + lookup_alias
                    ),
                )
            else:
                field = namespace.get(filter_name, Field(None))
            # NOTE field should always be pydantic FieldInfo, convince type checker
            assert field is not None
            field.json_schema_extra = {"sqla_column": name}
            namespace[filter_name] = field


ExpandType = TypeVar("ExpandType", str, list[str])
FilterType = TypeVar("FilterType")


class BaseFilter(BaseModel, metaclass=FilterMeta):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        populate_by_name=True,
    )
    sqla_model: ClassVar[type | None] = None

    @model_validator(mode="before")
    @classmethod
    def expand_simple_filters(
        cls, v: ExpandType | dict[str, ExpandType]
    ) -> dict[str, ExpandType]:
        return expand_simple_filter(v)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)

        except ValidationError as e:
            raise BadFilterArguments(model=e.title, errors=e.errors())

    def join(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        session: db.Session | None = None,
    ) -> db.sql.Select[tuple[FilterType]]:
        return exc

    def apply(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        model: object,
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        dict_model = dict(self)
        for name, field_info in self.__class__.model_fields.items():
            value = dict_model.get(name, field_info.get_default())
            if isinstance(value, BaseFilter):
                submodel = getattr(value, "sqla_model", None)
                model_getter = getattr(value, "get_sqla_model", None)
                if submodel is None and callable(model_getter):
                    submodel = model_getter(session)

                exc = value.join(exc, session=session)
                exc = value.apply(exc, submodel, session)
            elif value is not None:
                func_name = get_filter_func_name(name)
                filter_func = getattr(self, func_name, None)
                if filter_func is None:
                    raise ProgrammingError

                sqla_column: str | None = None
                if isinstance(field_info.json_schema_extra, dict):
                    jschema_col = field_info.json_schema_extra.get("sqla_column")
                    if not isinstance(jschema_col, str):
                        raise ProgrammingError(
                            "Field argument `sqla_column` must be of type `str`."
                        )
                    sqla_column = jschema_col
                else:
                    sqla_column = None
                column = (
                    None if sqla_column is None else getattr(model, sqla_column, None)
                )
                exc = filter_func(exc, column, value, session=session)
        return exc.distinct()


def expand_simple_filter(
    value: ExpandType | dict[str, ExpandType],
) -> dict[str, ExpandType]:
    if isinstance(value, str):
        return dict(name__like=value) if "*" in value else dict(name=value)
    elif isinstance(value, list):
        if any(["*" in v for v in value]):
            raise NotImplementedError("Filter by list with wildcard is not implemented")
        return dict(name__in=value)

    return value
