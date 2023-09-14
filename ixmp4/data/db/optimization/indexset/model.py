from typing import ClassVar

from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import base


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    name: types.String = db.Column(db.String(255), nullable=False, unique=True)
    elements: types.JsonList = db.Column(db.JsonType, nullable=False, default=[])

    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)

    @declared_attr
    def run__id(cls):
        return db.Column(
            db.Integer, db.ForeignKey("run.id"), nullable=False, index=True
        )

    @validates("elements")
    def validate_elements(self, key, value):
        unique = set()
        for element in value:
            if element in unique:
                raise ValueError(f"{element} already defined for IndexSet {self.name}!")
            else:
                unique.add(element)
        return value
