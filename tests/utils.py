import pandas.testing as pdt

from ixmp4 import Platform
from ixmp4.data.abstract.optimization import IndexSet


def assert_unordered_equality(df1, df2, **kwargs):
    df1 = df1.sort_index(axis=1)
    df1 = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
    df2 = df2.sort_index(axis=1)
    df2 = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
    pdt.assert_frame_equal(df1, df2, **kwargs)


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
