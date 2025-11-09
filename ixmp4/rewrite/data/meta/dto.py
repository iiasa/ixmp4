from ixmp4.rewrite.data.base.dto import BaseModel

from .type import Type

MetaValueType = bool | float | int | str


class RunMetaEntry(BaseModel):
    """Run meta entry model."""

    run__id: int
    "Foreign unique integer id of a run."
    key: str
    "Key for the entry. Unique for each `run__id`."
    dtype: Type
    "Datatype of the entry's value."
    value: MetaValueType
    "Value of the entry."

    value_int: int | None
    value_str: str | None
    value_float: float | None
    value_bool: bool | None

    def __str__(self) -> str:
        return f"<RunMetaEntry {self.id} run__id={self.run__id} \
            key={self.key} value={self.value}"
