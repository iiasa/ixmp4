from typing import Any, Iterable, Optional

from pydantic import BaseModel, Extra, Field, ValidationError

from ixmp4 import db
from ixmp4.core.exceptions import BadFilterArguments, ProgrammingError


def exact(c, v):
    return c == v


def in_(c, v):
    return c.in_(v)


def like(c, v):
    return c.like(v)


def ilike(c, v):
    return c.ilike(v)


def notlike(c, v):
    return c.notlike(v)


def notilike(c, v):
    return c.notilike(v)


def gt(c, v):
    return c > v


def lt(c, v):
    return c < v


def gte(c, v):
    return c >= v


def lte(c, v):
    return c <= v


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
lookup_map: dict[type, dict] = {
    Id: {
        "__root__": (int, exact),
        "in": (Iterable[int], in_),
    },
    Float: {
        "__root__": (int, exact),
        "in": (Iterable[int], in_),
        "gt": (int, gt),
        "lt": (int, lt),
        "gte": (int, gte),
        "lte": (int, lte),
    },
    Integer: {
        "__root__": (int, exact),
        "in": (Iterable[int], in_),
        "gt": (int, gt),
        "lt": (int, lt),
        "gte": (int, gte),
        "lte": (int, lte),
    },
    Boolean: {
        "__root__": (bool, exact),
    },
    String: {
        "__root__": (str, exact),
        "in": (Iterable[str], in_),
        "like": (str, like),
        "ilike": (str, ilike),
        "notlike": (str, notlike),
        "notilike": (str, notilike),
    },
}


def get_filter_func_name(n: str) -> str:
    return filter_func_prefix + n.strip()


PydanticMeta: type = type(BaseModel)


class FilterMeta(PydanticMeta):
    def __new__(cls, name, bases, namespace, **kwargs):
        field_types = namespace.get("__annotations__", {}).copy()
        for field_name, field_type in field_types.items():
            try:
                lookups = lookup_map[field_type]
            except KeyError:
                lookups = {}
                continue

            field = namespace.get(field_name, None)
            if field is not None:
                override_lookups = field.extra.get("lookups", None)
                if override_lookups:
                    lookups = {
                        k: v for k, v in lookups.items() if k in override_lookups
                    }
                elif override_lookups is None:
                    pass
                else:
                    lookups = {}
            cls.expand_lookups(
                field_name,
                lookups,
                namespace,
                None if field is None else field.alias,
            )

        return super().__new__(cls, name, bases, namespace, **kwargs)

    @classmethod
    def expand_lookups(
        cls, name, lookups, namespace, base_field_alias: str | None = None
    ):
        for lookup_alias, (type_, func) in lookups.items():
            if lookup_alias == "__root__":
                filter_name = name
            else:
                filter_name = name + argument_seperator + lookup_alias

            namespace["__annotations__"][filter_name] = Optional[type_]
            func_name = get_filter_func_name(filter_name)

            def filter_func(self, exc, f, v, func=func, session=None):
                return exc.where(func(f, v))

            namespace.setdefault(func_name, filter_func)

            field = namespace.get(filter_name, Field())
            field.extra.setdefault("sqla_column", name)
            if base_field_alias is not None and lookup_alias != "__root__":
                field.alias = base_field_alias + argument_seperator + lookup_alias
            namespace[filter_name] = field


class BaseFilter(BaseModel, metaclass=FilterMeta):
    class Config:
        extra = Extra.forbid
        sqla_model: type | None = None
        allow_population_by_field_name = True

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            raise BadFilterArguments(model=e.model.__name__, errors=e.errors())

    def join(self, exc: db.sql.Select, session=None) -> db.sql.Select:
        return exc

    def apply(self, exc: db.sql.Select, model, session) -> db.sql.Select:
        for name, field in self.__fields__.items():
            value = getattr(self, name, field.field_info.default)

            if isinstance(value, BaseFilter):
                submodel = getattr(value.Config, "sqla_model", None)
                model_getter = getattr(value.Config, "get_sqla_model", None)
                if submodel is None and callable(model_getter):
                    submodel = model_getter(session)

                exc = value.join(exc, session=session)
                exc = value.apply(exc, submodel, session)
            elif value is not None:
                func_name = get_filter_func_name(name)
                filter_func = getattr(self, func_name, None)
                if filter_func is None:
                    raise ProgrammingError

                sqla_column = field.field_info.extra.get("sqla_column")
                if sqla_column is None:
                    column = None
                else:
                    column = getattr(model, sqla_column, None)
                exc = filter_func(exc, column, value, session=session)
        return exc.distinct()
