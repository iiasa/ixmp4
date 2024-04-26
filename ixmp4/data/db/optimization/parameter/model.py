from typing import Any, ClassVar

import pandas as pd
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base


class Parameter(base.BaseModel, base.OptimizationDataMixin, base.UniqueNameRunIDMixin):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    # TODO Same as in table/model.py
    columns: types.Mapped[list["Column"]] = db.relationship()  # type: ignore

    @validates("data")
    def validate_data(self, key, data: dict[str, Any]):
        data_frame: pd.DataFrame = pd.DataFrame.from_dict(data)
        data_frame_to_validate = data_frame.drop(columns=["values", "units"])

        self._validate_data(data_frame=data_frame_to_validate, data=data)
        return data_frame.to_dict(orient="list")
