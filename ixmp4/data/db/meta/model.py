from typing import ClassVar

from ixmp4 import db
from ixmp4.core.exceptions import InvalidRunMeta
from ixmp4.data import abstract, types

from .. import base


class RunMetaEntry(base.BaseModel):
    NotFound: ClassVar = abstract.RunMetaEntry.NotFound
    NotUnique: ClassVar = abstract.RunMetaEntry.NotUnique
    DeletionPrevented: ClassVar = abstract.RunMetaEntry.DeletionPrevented

    Type: ClassVar = abstract.RunMetaEntry.Type

    _column_map = {
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
        "type",
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
    type: types.String = db.Column(db.String(20), nullable=False)

    value_int: types.Integer = db.Column(db.Integer, nullable=True)
    value_str: types.String = db.Column(db.String(1023), nullable=True)
    value_float: types.Float = db.Column(db.Float, nullable=True)
    value_bool: types.Boolean = db.Column(db.Boolean, nullable=True)

    @property
    def value(self) -> abstract.MetaValue:
        type_ = RunMetaEntry.Type(self.type)
        col = self._column_map[type_]
        return getattr(self, col)

    def __init__(self, *args, **kwargs) -> None:
        value = kwargs.pop("value")
        value_type = type(value)
        try:
            type_ = RunMetaEntry.Type.from_pytype(value_type)
            col = self._column_map[type_]
        except KeyError:
            raise InvalidRunMeta(
                f"Invalid type `{value_type}` for value of `RunMetaEntry`."
            )
        kwargs["type"] = type_
        kwargs[col] = value
        super().__init__(*args, **kwargs)
