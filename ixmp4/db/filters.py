import operator
from collections.abc import Callable, Iterable
from types import GenericAlias, UnionType
from typing import (
    Any,
    ClassVar,
    Optional,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic.fields import FieldInfo

# TODO Import these from typing when dropping support for 3.10
from typing_extensions import Protocol, Self, TypedDict

from ixmp4 import db
from ixmp4.core.exceptions import BadFilterArguments, ProgrammingError

in_Type = TypeVar("in_Type")


class RemotePathStep(TypedDict):
    """Type definition for a single step in a remote filter join path."""

    target_model: type
    fk_attr: str
    source_model: type
    pk_attr: str


class RemoteFilterConfig(Protocol):
    """Protocol for classes that support remote filter optimization."""

    _remote_filters: set[str]
    _remote_path: list[RemotePathStep]


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


Boolean = bool
Float = float  # An explicit proxy type for `float`.
Id = int  # A no-op type for a reduced set of `Integer` lookups.
Integer = int  # An explicit proxy type for `int`.
String = str  # An explicit proxy type for `str`.


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
        annots: dict[str, type]
        # Use Any since annotationlib is only available on Python 3.14
        # wrapped_annotate: Callable[[Any], dict[str, Any]] | None

        if sys.version_info >= (3, 14):
            import annotationlib

            if annotate := annotationlib.get_annotate_from_class_namespace(namespace):
                annots = annotationlib.call_annotate_function(
                    annotate, format=annotationlib.Format.FORWARDREF
                )

                # namespace["__filter_names__"]: dict[str, type] = {}

                # def wrapped_annotate(format: annotationlib.Format) -> dict[str, Any]:
                #     _annots = annotationlib.call_annotate_function(
                #         annotate, format, owner=new_cls
                #     )
                #     combined_annots = {
                #         **namespace.get("__filter_names__", {}),
                #         **_annots,
                #     }
                #     # print("combined_annots: ")
                #     # print(combined_annots)
                #     return combined_annots
            else:
                annots = {}
                # wrapped_annotate = None

        else:
            annots = namespace.get("__annotations__", {}).copy()
            # wrapped_annotate = None
        for _name, annot in annots.items():
            if get_origin(annot) == ClassVar:
                continue
            cls.process_field(namespace, _name, annot)

        # if sys.version_info >= (3, 14):
        #     for filter_name, annotation in namespace.get(
        #         "__filter_names__", {}
        #     ).items():
        #         namespace[filter_name].annotation = annotation

        #     if annotate:

        #         def wrapped_annotate(format: annotationlib.Format) -> dict[str, Any]:
        #             _annots = annotationlib.call_annotate_function(annotate, format)
        #             combined_annots = {
        #                 **namespace.get("__filter_names__", {}),
        #                 **_annots,
        #             }
        #             # print("combined_annots: ")
        #             # print(combined_annots)
        #             return combined_annots

        #         namespace["__annotate_func__"] = wrapped_annotate
        # print("namespace after filter_name:")
        # print(namespace)
        # if _annotate := annotationlib.get_annotate_from_class_namespace(namespace):
        #     _raw_annots = annotationlib.call_annotate_function(
        #         _annotate, format=annotationlib.Format.FORWARDREF
        #     )
        # else:
        #     _raw_annots = {}
        # print("raw annotations:")
        # print(_raw_annots)

        new_cls = cast(
            FilterMeta, super().__new__(cls, name, bases, namespace, **kwargs)
        )

        # if wrapped_annotate is not None:
        #     new_cls.__annotate__ = wrapped_annotate

        return new_cls

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
                        v: (
                            Integer
                            | Float
                            | Id
                            | String
                            | Boolean
                            | list[Integer | Float | Id | String | Boolean]
                        ),
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

            if sys.version_info < (3, 14):
                namespace["__annotations__"][filter_name] = Optional[type_]
            # else:
            #     namespace["__filter_names__"][filter_name] = Optional[type_]

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
            json_schema_extra = {"sqla_column": name}
            field.json_schema_extra = json_schema_extra
            field._attributes_set["json_schema_extra"] = json_schema_extra

            if sys.version_info >= (3, 14):
                field.annotation = Optional[type_]

            namespace[filter_name] = field


ExpandType = TypeVar("ExpandType", str, list[str])
FilterType = TypeVar("FilterType")


class BaseFilter(BaseModel, metaclass=FilterMeta):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        validate_by_alias=True,
        validate_by_name=True,
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
        if self._should_use_subquery_optimization():
            return self._apply_with_subquery_optimization(exc, model, session)

        return self._apply(exc, model, session)

    def _should_use_subquery_optimization(self) -> bool:
        """Check if subquery optimization should be used for this filter."""
        remote_filters = getattr(self, "_remote_filters", None)
        if not remote_filters:
            return False

        for name in remote_filters:
            field_info = self.__class__.model_fields.get(name)
            if field_info:
                value = getattr(self, name, field_info.get_default())
                if isinstance(value, BaseFilter) and self._is_filter_active(value):
                    return True
        return False

    def _is_filter_active(self, filter_value: "BaseFilter") -> bool:
        """Check if a filter has any active (non-None) values."""
        field_values = filter_value.model_dump(exclude_none=True)
        return bool(field_values)

    def _apply(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        model: object,
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        """Standard apply logic."""
        for name, field_info in self.__class__.model_fields.items():
            value = getattr(self, name, field_info.get_default())
            if isinstance(value, BaseFilter):
                exc = self._apply_nested_filter(exc, value, session)
            elif value is not None:
                exc = self._apply_field_filter(exc, name, value, model, session)
        return exc.distinct()

    def _apply_without_distinct(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        model: object,
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply filters without adding DISTINCT (for use in subqueries)."""
        for name, field_info in self.__class__.model_fields.items():
            value = getattr(self, name, field_info.get_default())
            if isinstance(value, BaseFilter):
                exc = self._apply_nested_filter(exc, value, session)
            elif value is not None:
                exc = self._apply_field_filter(exc, name, value, model, session)
        return exc

    def _apply_with_subquery_optimization(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        model: object,
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply filters using subquery optimization for remote filters."""
        remote_filters: set[str] = getattr(self, "_remote_filters", set())
        remote_path: list[RemotePathStep] | None = getattr(self, "_remote_path", None)
        if not remote_path:
            raise ProgrammingError(
                f"Filter {self.__class__.__name__} defines _remote_filters "
                "but missing _remote_path"
            )
        if model is None:
            return self._apply(exc, model, session)
        active_remote_filters = self._collect_active_remote_filters(remote_filters)
        if active_remote_filters:
            exc = self._apply_subquery_filter(
                exc, model, session, active_remote_filters, remote_path
            )
        exc = self._apply_local_filters(exc, model, session, remote_filters)
        return exc.distinct()

    def _collect_active_remote_filters(
        self, remote_filters: set[str]
    ) -> dict[str, "BaseFilter"]:
        """Collect remote filters that are active."""
        active_remote_filters = {}
        for name in remote_filters:
            value = getattr(self, name, None)
            if isinstance(value, BaseFilter) and self._is_filter_active(value):
                active_remote_filters[name] = value
        return active_remote_filters

    def _get_model_id_column(self, model: object) -> Any:
        """Safely get the ID column from a model class."""
        if not hasattr(model, "id"):
            raise ProgrammingError(
                f"Model {model.__class__.__name__} missing required 'id' column"
            )
        id_column = getattr(model, "id")
        if not hasattr(id_column, "type"):
            raise ProgrammingError(
                f"Model {model.__class__.__name__}.id is not a SQLAlchemy column"
            )
        return id_column

    def _apply_subquery_filter(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        model: object,
        session: db.Session,
        active_remote_filters: dict[str, "BaseFilter"],
        remote_path: list[RemotePathStep],
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply remote filters using subquery."""
        current_id_column = self._get_model_id_column(model)
        subquery = db.sql.select(current_id_column)
        subquery = self._build_subquery_joins(subquery, remote_path)
        subquery = self._apply_filters_to_subquery(
            subquery, active_remote_filters, session
        )
        model_id_column = self._get_model_id_column(
            getattr(self, "sqla_model", None) or model
        )
        return exc.where(model_id_column.in_(subquery.scalar_subquery()))

    def _build_subquery_joins(
        self,
        subquery: db.sql.Select[tuple[FilterType]],
        remote_path: list[RemotePathStep],
    ) -> db.sql.Select[tuple[FilterType]]:
        """Build the join chain for the subquery."""
        for step in remote_path:
            target_model = step["target_model"]
            fk_attr = step["fk_attr"]
            source_model = step["source_model"]
            pk_attr = step["pk_attr"]

            fk_column = getattr(target_model, fk_attr)
            pk_column = getattr(source_model, pk_attr)
            join_condition = fk_column == pk_column
            subquery = subquery.join(target_model, join_condition)
        return subquery

    def _apply_filters_to_subquery(
        self,
        subquery: db.sql.Select[tuple[FilterType]],
        active_remote_filters: dict[str, "BaseFilter"],
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply remote filters to the subquery."""
        for filter_value in active_remote_filters.values():
            subquery = filter_value.join(subquery, session=session)
            # We don't want DISTINCT in the subquery
            # it is potentially very costly and not needed
            subquery = filter_value._apply_without_distinct(
                subquery, filter_value.sqla_model, session
            )
        return subquery

    def _apply_local_filters(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        model: object,
        session: db.Session,
        remote_filters: set[str],
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply local (non-remote) filters."""
        for name, field_info in self.__class__.model_fields.items():
            if name in remote_filters:
                continue
            value = getattr(self, name, field_info.get_default())
            if isinstance(value, BaseFilter):
                exc = self._apply_nested_filter(exc, value, session)
            elif value is not None:
                exc = self._apply_field_filter(exc, name, value, model, session)
        return exc

    def _apply_nested_filter(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        filter_value: "BaseFilter",
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply a nested filter."""
        submodel = getattr(filter_value, "sqla_model", None)
        model_getter = getattr(filter_value, "get_sqla_model", None)
        if submodel is None and callable(model_getter):
            submodel = model_getter(session)

        exc = filter_value.join(exc, session=session)
        return filter_value.apply(exc, submodel, session)

    def _apply_field_filter(
        self,
        exc: db.sql.Select[tuple[FilterType]],
        name: str,
        value: Any,
        model: object,
        session: db.Session,
    ) -> db.sql.Select[tuple[FilterType]]:
        """Apply a field-level filter."""
        func_name = get_filter_func_name(name)
        filter_func = getattr(self, func_name, None)
        if filter_func is None:
            raise ProgrammingError

        sqla_column = self._get_sqla_column(name)
        column = None if sqla_column is None else getattr(model, sqla_column, None)
        result = filter_func(exc, column, value, session=session)
        if not isinstance(result, db.sql.Select):
            raise ProgrammingError("Filter function must return a Select query")
        return result

    def _get_sqla_column(self, name: str) -> str | None:
        """Get the SQLAlchemy column name for a field."""
        field_info = self.__class__.model_fields.get(name)
        if field_info and isinstance(field_info.json_schema_extra, dict):
            jschema_col = field_info.json_schema_extra.get("sqla_column")
            if not isinstance(jschema_col, str):
                raise ProgrammingError(
                    "Field argument `sqla_column` must be of type `str`."
                )
            return jschema_col
        return None


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
