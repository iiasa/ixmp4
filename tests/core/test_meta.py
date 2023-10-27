import pandas as pd
import pandas.testing as pdt

from ..utils import all_platforms


@all_platforms
def test_run_meta(test_mp):
    run1 = test_mp.Run(
        "Model",
        "Scenario",
        version="new",
    )
    run1.set_as_default()

    # set and update different types of meta indicators
    run1.meta = {"mint": 13, "mfloat": 0.0, "mstr": "foo"}
    run1.meta["mfloat"] = -1.9

    run2 = test_mp.Run("Model", "Scenario")

    # assert meta by run
    assert dict(run2.meta) == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}
    assert dict(run1.meta) == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}

    # assert meta via platform
    exp = pd.DataFrame(
        [[1, "mint", 13], [1, "mstr", "foo"], [1, "mfloat", -1.9]],
        columns=["run_id", "key", "value"],
    )
    pdt.assert_frame_equal(test_mp.meta.tabulate(run_id=1), exp)

    # remove all meta indicators and set a new indicator
    run1.meta = {"mnew": "bar"}

    run2 = test_mp.Run("Model", "Scenario")

    # assert meta by run
    assert dict(run2.meta) == {"mnew": "bar"}
    assert dict(run1.meta) == {"mnew": "bar"}

    # assert meta via platform
    exp = pd.DataFrame([[1, "mnew", "bar"]], columns=["run_id", "key", "value"])
    pdt.assert_frame_equal(test_mp.meta.tabulate(run_id=1), exp)

    del run1.meta["mnew"]
    run2 = test_mp.Run("Model", "Scenario")

    # assert meta by run
    assert dict(run2.meta) == {}
    assert dict(run1.meta) == {}

    # assert meta via platform
    exp = pd.DataFrame([], columns=["run_id", "key", "value"])
    pdt.assert_frame_equal(test_mp.meta.tabulate(run_id=1), exp)
