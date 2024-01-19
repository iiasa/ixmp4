from typing import ClassVar

from sqlalchemy import UniqueConstraint

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base


class Table(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    constrained_to_indexsets: ClassVar[list[str] | None] = None

    name: types.String = db.Column(db.String(255), nullable=False, unique=False)
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    columns: types.Mapped[list["Column"]] = db.relationship()

    # NOTE: Could create a RunMixin for all optimization models, see
    # https://docs.sqlalchemy.org/en/20/orm/declarative_mixins.html#mixing-in-relationships
    run__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("run.id"), nullable=False, index=True
    )

    __table_args__ = (UniqueConstraint(name, "run__id"),)

    # NOTE: This could probably also be a Mixin across almost all models
    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)

    # Table needs to validate data
    # we might need
    def get_indexset_ids(self):
        ...

    # and
    def validate_data(self):
        ...

    # def __init__(self, *args, **kwargs) -> None:
    #     constrained_to_indexsets: list[str] | None = kwargs.pop("constrained_to_indexsets", None) # noqa
    #     if constrained_to_indexsets:
    #         indexset_names = constrained_to_indexsets
    #     else:

    #     value_type = type(value)
    #     try:
    #         type_ = RunMetaEntry.Type.from_pytype(value_type)
    #         col = self._column_map[type_]
    #     except KeyError:
    #         raise InvalidRunMeta(
    #             f"Invalid type `{value_type}` for value of `RunMetaEntry`."
    #         )
    #     kwargs["type"] = type_
    #     kwargs[col] = value
    # super().__init__(*args, **kwargs)

    # def add_column(self, name: str, dtype: str, indexset: IndexSet):
    #     Column(name=name, dtype=dtype, table=self, indexset=indexset)

    # @validates("columns")
    # def validate_columns(self, key, value):
    #     return value

    # THOUGHTS: We want to create a Table by just passing in some data. According to
    # https://docs.messageix.org/projects/ixmp/en/stable/data-model.html#data-associated-with-a-scenario-object
    # the data needs to have dimensions, where each dimension is linked to an Indexset,
    # and can havean optional name:
    # Indexset  Indexset 2
    # "one"     2           <- this set is called a Key
    # "two"     1           <- this, too
    # for example.
    # In addition, it used to contain bools because each element of an Indexset (or
    # each Key) either is or is not a member of the Table.
    # Questions: how does the linkage work? If we pass in data without a name, how is
    # the dimension inferred? Should we make it mandatory to pass in a name and require
    # that the name correspond to an Indexset defined for the same Run?
    # What do we need the additional bools for?
