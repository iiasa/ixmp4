# import inspect
from typing import Any, ClassVar, Iterable, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from typing_extensions import Annotated

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


Integer = Annotated[int, Field(description="An explicit proxy type for `int`.")]


Float = Annotated[float, Field(description="An explicit proxy type for `float`.")]


Id = Annotated[
    int, Field(description="A no-op type for a reduced set of `Integer` lookups.")
]


String = Annotated[str, Field(description="An explicit proxy type for `str`.")]


Boolean = bool

argument_seperator = "__"
filter_func_prefix = "filter_"
lookup_map: dict[object, dict] = {
    Union[Id, None]: {
        "__root__": (int, exact),
        "in": (Iterable[int], in_),
    },
    Union[Float, None]: {
        "__root__": (int, exact),
        "in": (Iterable[int], in_),
        "gt": (int, gt),
        "lt": (int, lt),
        "gte": (int, gte),
        "lte": (int, lte),
    },
    Union[Integer, None]: {
        "__root__": (int, exact),
        "in": (Iterable[int], in_),
        "gt": (int, gt),
        "lt": (int, lt),
        "gte": (int, gte),
        "lte": (int, lte),
    },
    Union[Boolean, None]: {
        "__root__": (bool, exact),
    },
    Union[String, None]: {
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
        # field_types = inspect.get_annotations(cls)
        field_types = namespace.get("__annotations__", {}).copy()
        for field_name, field_type in field_types.items():
            try:
                lookups = lookup_map[field_type]
            except KeyError:
                lookups = {}
                continue

            field = namespace.get(field_name, None)
            if field is not None:
                if isinstance(field.json_schema_extra, dict):
                    override_lookups = field.json_schema_extra.get("lookups", None)
                else:
                    override_lookups = None
                if override_lookups:
                    lookups = {
                        k: v for k, v in lookups.items() if k in override_lookups
                    }
                elif override_lookups is None:
                    pass
                else:
                    lookups = {}
                base_field_alias = str(field.alias) if field.alias else field_name
            else:
                base_field_alias = None
            cls.expand_lookups(
                field_name,
                lookups,
                namespace,
                base_field_alias,
            )

        return super().__new__(cls, name, bases, namespace, **kwargs)

    @classmethod
    def expand_lookups(
        cls, name: str, lookups: dict, namespace, base_field_alias: str | None = None
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

            field = namespace.get(filter_name, Field(None))
            field.json_schema_extra = {"sqla_column": name}
            if base_field_alias is not None and lookup_alias != "__root__":
                field.alias = base_field_alias + argument_seperator + lookup_alias
            namespace[filter_name] = field


class BaseFilter(BaseModel, metaclass=FilterMeta):
    model_config = ConfigDict(
        extra="forbid", populate_by_name=True, arbitrary_types_allowed=True
    )
    sqla_model: ClassVar[type | None] = None

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except ValidationError as e:
            raise BadFilterArguments(model=e.title, errors=e.errors())

    def join(self, exc: db.sql.Select, session=None) -> db.sql.Select:
        return exc

    def apply(self, exc: db.sql.Select, model, session) -> db.sql.Select:
        for name, field_info in self.model_fields.items():
            value = getattr(self, name, field_info.get_default())

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

                if isinstance(field_info.json_schema_extra, dict):
                    sqla_column = field_info.json_schema_extra.get("sqla_column")
                else:
                    sqla_column = None
                if sqla_column is None:
                    column = None
                else:
                    column = getattr(model, sqla_column, None)
                exc = filter_func(exc, column, value, session=session)
        return exc.distinct()
