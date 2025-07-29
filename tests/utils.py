from collections.abc import Generator
from contextlib import contextmanager, nullcontext
from itertools import chain
from typing import Any

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

# Import this from typing when dropping 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4 import Platform
from ixmp4.data.abstract.optimization import IndexSet


# Based on current usage
class AssertKwargs(TypedDict, total=False):
    check_like: bool
    check_dtype: bool


def clean_df(df: pd.DataFrame):
    df = df.sort_index(axis=1)
    df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    if "step_datetime" in df.columns:
        df["step_datetime"] = pd.to_datetime(df["step_datetime"])
    if "step_year" in df.columns:
        df["step_year"] = df["step_year"].fillna(np.nan).map(float)
    return df


def assert_unordered_equality(
    df1: pd.DataFrame, df2: pd.DataFrame, **kwargs: Unpack[AssertKwargs]
) -> None:
    pdt.assert_frame_equal(
        clean_df(df1),
        clean_df(df2),
        **kwargs
    )


def create_indexsets_for_run(
    platform: Platform, run_id: int, amount: int = 2, offset: int = 1
) -> tuple[IndexSet, ...]:
    """Create `amount` indexsets called `Indexset n` for `run`."""
    return tuple(
        platform.backend.optimization.indexsets.create(
            run_id=run_id, name=f"Indexset {i}"
        )
        for i in range(offset, offset + amount)
    )


# Thanks to khaeru for writing this for ixmp
@contextmanager
def assert_logs(
    caplog: pytest.LogCaptureFixture,
    message_or_messages: str | list[str],
    at_level: int | str | None = None,
) -> Generator[None, Any, None]:
    """Assert that *message_or_messages* appear in logs.

    Use assert_logs as a context manager for a statement that is expected to trigger
    certain log messages. assert_logs checks that these messages are generated.

    Example
    -------

    def test_foo(caplog):
        with assert_logs(caplog, 'a message'):
            logging.getLogger(__name__).info('this is a message!')

    Parameters
    ----------
    caplog : pytest.LogCaptureFixture
        The pytest caplog fixture.
    message_or_messages : str or list of str
        String(s) that must appear in log messages.
    at_level : int or str, optional
        Messages must appear on 'ixmp4' or a sub-logger with at least this level.
    """
    __tracebackhide__ = True

    # Wrap a string in a list
    expected = (
        [message_or_messages]
        if isinstance(message_or_messages, str)
        else message_or_messages
    )

    # Record the number of records prior to the managed block
    first = len(caplog.records)

    # Use the pytest caplog fixture's built-in context manager to temporarily set
    # the level of the 'ixmp4' logger if a specific level is requested
    # Otherwise, ctx does nothing
    ctx = (
        caplog.at_level(at_level, logger="ixmp4")
        if at_level is not None
        else nullcontext()
    )

    try:
        with ctx:
            yield  # Nothing provided to the managed block
    finally:
        # List of bool indicating whether each of `expected` was found
        found = [any(e in msg for msg in caplog.messages[first:]) for e in expected]

        if not all(found):
            # Format a description of the missing messages
            lines = chain(
                ["Did not log:"],
                [f"    {repr(msg)}" for i, msg in enumerate(expected) if not found[i]],
                ["among:"],
                ["    []"]
                if len(caplog.records) == first
                else [f"    {repr(msg)}" for msg in caplog.messages[first:]],
            )
            pytest.fail("\n".join(lines))
