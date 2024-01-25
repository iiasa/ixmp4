from typing import ClassVar

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import base


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    name: types.String = db.Column(db.String(255), nullable=False, unique=False)
    elements: types.JsonList = db.Column(db.JsonType, nullable=False, default=[])

    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)

    run__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("run.id"), nullable=False, index=True
    )

    __table_args__ = (UniqueConstraint(name, run__id),)

    @validates("elements")
    def validate_elements(self, key, value):
        unique = set()
        for element in value:
            if element in unique:
                raise ValueError(f"{element} already defined for IndexSet {self.name}!")
            else:
                unique.add(element)
        return value
