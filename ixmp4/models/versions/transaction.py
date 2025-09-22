from sqlalchemy import DateTime

from ixmp4 import db
from ixmp4.core.exceptions import NotFound
from ixmp4.data import types

from .. import base


class Transaction(base.BaseModel):
    NotFound = NotFound
    __tablename__ = "transaction"

    issued_at: types.DateTime = db.Column(DateTime())
