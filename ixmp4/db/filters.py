from types import UnionType
from typing import Any, ClassVar, Iterable, Optional, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.fields import FieldInfo

from ixmp4 import db
from ixmp4.core.exceptions import BadFilterArguments, ProgrammingError


def exact(c, v):
    return c == v


def in_(c, v):
    return c.in_(v)


def like(c, v):
    return c.like(escape_wildcard(v), escape="\\")


def ilike(c, v):
    return c.ilike(escape_wildcard(v), escape="\\")


def notlike(c, v):
    return c.notlike(escape_wildcard(v), escape="\\")


def notilike(c, v):
    return c.notilike(escape_wildcard(v), escape="\\")


def gt(c, v):
    return c > v


def lt(c, v):
    return c < v


def gte(c, v):
    return c >= v


def lte(c, v):
    return c <= v


def escape_wildcard(v):
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
lookup_map: dict[object, dict] = {
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
    def __new__(cls, name: str, bases: tuple, namespace: dict, **kwargs):
        annots = namespace.get("__annotations__", {}).copy()
        for name, annot in annots.items():
            if get_origin(annot) == ClassVar:
                continue
            cls.process_field(namespace, name, annot)

        return super().__new__(cls, name, bases, namespace, **kwargs)

    @classmethod
    def build_lookups(cls, field_type: type) -> dict:
        global lookup_map
        if field_type not in lookup_map.keys():
            if get_origin(field_type) in [Union, UnionType]:
                unified_types = get_args(field_type)
                ut_lookup_map = {
                    type_: cls.build_lookups(type_) for type_ in unified_types
                }
                all_lookup_names = set()
                for tl in ut_lookup_map.values():
                    all_lookup_names |= set(tl.keys())

                lookups = {}
                for lookup_name in all_lookup_names:
                    tuples = [
                        ut_lookup_map[type_][lookup_name]
                        for type_ in unified_types
                        if ut_lookup_map[type_].get(lookup_name, None) is not None
                    ]
                    types, _ = zip(*tuples)

                    def lookup_func(c, v, tuples=tuples):
                        for t, lf in tuples:
                            if isinstance(v, t):
                                return lf(c, v)
                        raise ProgrammingError

                    lookups[lookup_name] = (
                        # dynamic union types can't
                        # be done according to type checkers
                        Union[tuple(types)],  # type:ignore
                        lookup_func,
                    )
                return lookups
            else:
                return {"__root__": (field_type, exact)}
        else:
            return lookup_map[field_type]

    @classmethod
    def process_field(cls, namespace: dict, field_name: str, field_type: type):
        lookups = cls.build_lookups(field_type)
        field: FieldInfo | None = namespace.get(field_name, Field(default=None))

        if field is None:
            return

        namespace.setdefault(field_name, field)
        if isinstance(field.json_schema_extra, dict):
            override_lookups = field.json_schema_extra.get("lookups", None)
        else:
            override_lookups = None
        if override_lookups:
            lookups = {k: v for k, v in lookups.items() if k in override_lookups}
        elif override_lookups is None:
            pass
        else:
            lookups = {}
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
        lookups: dict,
        namespace: dict,
        base_field_alias: str | None = None,
    ):
        global argument_seperator
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
            field.json_schema_extra = {"sqla_column": name}
            namespace[filter_name] = field


class BaseFilter(BaseModel, metaclass=FilterMeta):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        populate_by_name=True,
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
