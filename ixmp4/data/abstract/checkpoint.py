from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data.abstract import annotations

from . import base


class EnumerateKwargs(
    annotations.HasRunIdFilter,
    annotations.HasTransactionIdFilter,
    total=False,
):
    pass


class Checkpoint(base.BaseModel, Protocol):
    """Run checkpoint model.
    Represents a run's state (the data it holds) at a particular transaction.
    Used to roll back to a previous state of the run."""

    run__id: int
    "Id of associated run."

    transaction__id: int
    "Id of associated transaction."

    message: str
    "Checkpoint message."

    def __str__(self) -> str:
        return f"<Checkpoint {self.id} message={self.message}>"


class CheckpointRepository(
    base.Creator,
    base.Deleter,
    base.Enumerator,
    Protocol,
):
    def create(self, run__id: int, message: str) -> Checkpoint:
        """Creates a checkpoint.

        Parameters
        ----------
        run__id: int
            Id of associated run.

        transaction__id: int
            Id of associated transaction.

        message: str
            Checkpoint message.

        Raises
        ------
        :class:`ixmp4.data.abstract.checkpoint.Checkpoint.NotUnique`:
            If the checkpoints combination of `run__id` and
            `transaction__id` is not unique.

        Returns
        -------
        :class:`ixmp4.data.abstract.Model`:
            The created model.
        """
        ...

    def delete(self, id: int) -> Checkpoint:
        """Deletes a checkpoint by its id.

        Parameters
        ----------
        id : int
            The id of the checkpoint.

        Raises
        ------
        :class:`ixmp4.data.abstract.Checkpoint.NotFound`:
            If the checkpoint does not exist.

        """
        ...

    def get_by_id(self, id: int) -> Checkpoint:
        """Retrieves a checkpoint by its id.

        Parameters
        ----------
        id : int
            The id of the checkpoint.

        Raises
        ------
        :class:`ixmp4.data.abstract.Checkpoint.NotFound`:
            If the checkpoint does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Checkpoint`:
            The retrieved checkpoint.
        """
        ...

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Checkpoint]:
        r"""Lists checkpoints by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.checkpoint.filter.ModelFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Model`]:
            List of Model.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate checkpoints by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.checkpoint.filter.ModelFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name

        """
        ...
