import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4

EXP_META_COLS = ["model", "scenario", "version", "key", "value"]


def test_run_meta(platform: ixmp4.Platform) -> None:
    run1 = platform.runs.create("Model 1", "Scenario 1")
    run1.set_as_default()

    # set and update different types of meta indicators
    # NOTE mypy doesn't support setters taking a different type than their property
    # https://github.com/python/mypy/issues/3004
    run1.meta = {"mint": 13, "mfloat": 0.0, "mstr": "foo"}  # type: ignore[assignment]
    run1.meta["mfloat"] = -1.9

    run2 = platform.runs.get("Model 1", "Scenario 1")

    # assert meta by run
    assert dict(run2.meta) == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}
    assert dict(run1.meta) == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}

    # assert meta via platform
    exp = pd.DataFrame(
        [
            ["Model 1", "Scenario 1", 1, "mint", 13],
            ["Model 1", "Scenario 1", 1, "mstr", "foo"],
            ["Model 1", "Scenario 1", 1, "mfloat", -1.9],
        ],
        columns=EXP_META_COLS,
    )
    pdt.assert_frame_equal(platform.meta.tabulate(run_id=1), exp)

    # remove all meta indicators and set a new indicator
    run1.meta = {"mnew": "bar"}  # type: ignore[assignment]

    run2 = platform.runs.get("Model 1", "Scenario 1")

    # assert meta by run
    assert dict(run2.meta) == {"mnew": "bar"}
    assert dict(run1.meta) == {"mnew": "bar"}

    # assert meta via platform
    exp = pd.DataFrame(
        [["Model 1", "Scenario 1", 1, "mnew", "bar"]], columns=EXP_META_COLS
    )
    pdt.assert_frame_equal(platform.meta.tabulate(run_id=1), exp)

    del run1.meta["mnew"]
    run2 = platform.runs.get("Model 1", "Scenario 1")

    # assert meta by run
    assert dict(run2.meta) == {}
    assert dict(run1.meta) == {}

    # assert meta via platform
    exp = pd.DataFrame([], columns=EXP_META_COLS)
    pdt.assert_frame_equal(platform.meta.tabulate(run_id=1), exp, check_dtype=False)

    run2 = platform.runs.create("Model 2", "Scenario 2")
    run1.meta = {"mstr": "baz"}  # type: ignore[assignment]
    run2.meta = {"mfloat": 3.1415926535897}  # type: ignore[assignment]

    # test default_only run filter
    exp = pd.DataFrame(
        [["Model 1", "Scenario 1", 1, "mstr", "baz"]], columns=EXP_META_COLS
    )
    # run={"default_only": True} is default
    pdt.assert_frame_equal(platform.meta.tabulate(), exp)

    exp = pd.DataFrame(
        [
            ["Model 1", "Scenario 1", 1, "mstr", "baz"],
            ["Model 2", "Scenario 2", 1, "mfloat", 3.1415926535897],
        ],
        columns=EXP_META_COLS,
    )
    pdt.assert_frame_equal(platform.meta.tabulate(run={"default_only": False}), exp)

    # test is_default run filter
    exp = pd.DataFrame(
        [["Model 1", "Scenario 1", 1, "mstr", "baz"]], columns=EXP_META_COLS
    )
    pdt.assert_frame_equal(platform.meta.tabulate(run={"is_default": True}), exp)

    exp = pd.DataFrame(
        [["Model 2", "Scenario 2", 1, "mfloat", 3.1415926535897]],
        columns=EXP_META_COLS,
    )
    pdt.assert_frame_equal(
        platform.meta.tabulate(run={"default_only": False, "is_default": False}), exp
    )

    # test filter by key
    run1.meta = {"mstr": "baz", "mfloat": 3.1415926535897}  # type: ignore[assignment]
    exp = pd.DataFrame(
        [["Model 1", "Scenario 1", 1, "mstr", "baz"]], columns=EXP_META_COLS
    )
    pdt.assert_frame_equal(platform.meta.tabulate(key="mstr"), exp)


@pytest.mark.parametrize(
    "npvalue1, pyvalue1, npvalue2, pyvalue2",
    [
        (np.int64(1), 1, np.int64(13), 13),
        (np.float64(1.9), 1.9, np.float64(13.9), 13.9),
    ],
)
def test_run_meta_numpy(
    platform: ixmp4.Platform,
    npvalue1: np.int64 | np.float64,
    pyvalue1: int | float,
    npvalue2: np.int64 | np.float64,
    pyvalue2: int | float,
) -> None:
    """Test that numpy types are cast to simple types"""
    run1 = platform.runs.create("Model", "Scenario")
    run1.set_as_default()

    # set multiple meta indicators of same type ("value"-column of numpy-type)
    run1.meta = {"key": npvalue1, "other key": npvalue1}  # type: ignore[assignment]
    assert run1.meta["key"] == pyvalue1

    # set meta indicators of different types ("value"-column of type `object`)
    run1.meta = {"key": npvalue1, "other key": "some value"}  # type: ignore[assignment]
    assert run1.meta["key"] == pyvalue1

    # set meta via setter
    run1.meta["key"] = npvalue2
    assert run1.meta["key"] == pyvalue2

    # assert that meta values were saved and updated correctly
    run2 = platform.runs.get("Model", "Scenario")
    assert dict(run2.meta) == {"key": pyvalue2, "other key": "some value"}


@pytest.mark.parametrize("nonevalue", (None, np.nan))
def test_run_meta_none(platform: ixmp4.Platform, nonevalue: float | None) -> None:
    """Test that None-values are handled correctly"""
    run1 = platform.runs.create("Model", "Scenario")
    run1.set_as_default()

    # set multiple indicators where one value is None
    run1.meta = {"mint": 13, "mnone": nonevalue}  # type: ignore[assignment]
    assert run1.meta["mint"] == 13
    with pytest.raises(KeyError, match="'mnone'"):
        run1.meta["mnone"]

    assert dict(platform.runs.get("Model", "Scenario").meta) == {"mint": 13}

    # delete indicator via setter
    run1.meta["mint"] = nonevalue
    with pytest.raises(KeyError, match="'mint'"):
        run1.meta["mint"]

    assert not dict(platform.runs.get("Model", "Scenario").meta)


def test_platform_meta_empty(platform: ixmp4.Platform) -> None:
    """Test that an empty dataframe is returned if there are no scenarios"""
    exp = pd.DataFrame([], columns=["model", "scenario", "version", "key", "value"])
    pdt.assert_frame_equal(platform.meta.tabulate(), exp)
