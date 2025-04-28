from typing import ClassVar, cast

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4 import db
from ixmp4.core.exceptions import InvalidRunMeta
from ixmp4.data import abstract, types

from .. import base


class InitKwargs(TypedDict):
    run__id: int
    key: str
    value: abstract.annotations.PrimitiveTypes


class RunMetaEntry(base.BaseModel):
    __versioned__ = {}

    NotFound: ClassVar = abstract.RunMetaEntry.NotFound
    NotUnique: ClassVar = abstract.RunMetaEntry.NotUnique
    DeletionPrevented: ClassVar = abstract.RunMetaEntry.DeletionPrevented

    Type: ClassVar = abstract.RunMetaEntry.Type

    _column_map: dict[str, str] = {
        abstract.RunMetaEntry.Type.INT: "value_int",
        abstract.RunMetaEntry.Type.STR: "value_str",
        abstract.RunMetaEntry.Type.FLOAT: "value_float",
        abstract.RunMetaEntry.Type.BOOL: "value_bool",
    }

    __table_args__ = (
        db.UniqueConstraint(
            "run__id",
            "key",
        ),
    )
    updateable_columns = [
        "dtype",
        "value_int",
        "value_str",
        "value_float",
        "value_bool",
    ]

    run__id: types.Integer = db.Column(
        db.Integer,
        db.ForeignKey("run.id"),
        nullable=False,
        index=True,
    )
    run = db.relationship(
        "Run",
        backref="meta",
        foreign_keys=[run__id],
    )

    key: types.String = db.Column(db.String(1023), nullable=False)
    dtype: types.String = db.Column(db.String(20), nullable=False)

    value_int: types.Integer = db.Column(db.Integer, nullable=True)
    value_str: types.String = db.Column(db.String(1023), nullable=True)
    value_float: types.Float = db.Column(db.Float, nullable=True)
    value_bool: types.Boolean = db.Column(db.Boolean, nullable=True)

    @property
    def value(self) -> abstract.MetaValue:
        type_ = RunMetaEntry.Type(self.dtype)
        col = self._column_map[type_]
        value: abstract.MetaValue = getattr(self, col)
        return value

    def __init__(self, **kwargs: Unpack[InitKwargs]) -> None:
        _kwargs = cast(dict[str, abstract.annotations.PrimitiveTypes], kwargs)
        value = _kwargs.pop("value")
        value_type = type(value)
        try:
            type_ = RunMetaEntry.Type.from_pytype(value_type)
            col = self._column_map[type_]
        except KeyError:
            raise InvalidRunMeta(
                f"Invalid type `{value_type}` for value of `RunMetaEntry`."
            )
        _kwargs["dtype"] = type_
        _kwargs[col] = value
        super().__init__(**_kwargs)
