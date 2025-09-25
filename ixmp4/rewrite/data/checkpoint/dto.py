from ixmp4.rewrite.data.base.dto import BaseModel


class Checkpoint(BaseModel):
    """Run checkpoint data model."""

    run__id: int
    "Id of the run for this checkpoint."
    transaction__id: int | None
    "Id of the transaction for this checkpoint."
    message: str
    "Checkpoint message."

    def __str__(self) -> str:
        return (
            f"<Checkpoint {self.id} run__id={self.run__id} "
            f"transaction__id={self.transaction__id}>"
        )
