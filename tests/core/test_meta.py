import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from tests import backends
from tests.custom_exception import CustomException

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class MetaTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(self, platform: ixmp4.Platform) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        run.set_as_default()
        return run


class TestMetaData(MetaTest):
    def test_add_meta(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        with run.transact("Add meta data"):
            run.meta = {
                "mint": 13,
                "mfloat": 0.0,
                "mstr": "foo",
                "mnone": None,  # <- should be ignored
                "mnan": np.nan,  # <-'
            }
            run.meta["mfloat"] = -1.9

        assert dict(run.meta) == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}

        run2 = platform.runs.get("Model", "Scenario")
        assert dict(run2.meta) == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}

    def test_tabulate_platform_meta_after_add(self, platform: ixmp4.Platform) -> None:
        exp = pd.DataFrame(
            [
                ["Model", "Scenario", 1, "mfloat", -1.9],
                ["Model", "Scenario", 1, "mint", 13],
                ["Model", "Scenario", 1, "mstr", "foo"],
            ],
            columns=["model", "scenario", "version", "key", "value"],
        )
        ret = platform.meta.tabulate(run__id=1)
        pdt.assert_frame_equal(exp, ret, check_like=True)

        exp_str = pd.DataFrame(
            [
                ["Model", "Scenario", 1, "mstr", "foo"],
            ],
            columns=["model", "scenario", "version", "key", "value"],
        )
        ret_str = platform.meta.tabulate(key="mstr")
        pdt.assert_frame_equal(exp_str, ret_str, check_like=True)

    def test_delete_meta(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        with run.transact("Delete meta data with `del`"):
            del run.meta["mint"]

        assert dict(run.meta) == {"mstr": "foo", "mfloat": -1.9}

        with run.transact("Delete meta data with `None`"):
            run.meta["mfloat"] = None

        assert dict(run.meta) == {"mstr": "foo"}

        run2 = platform.runs.get("Model", "Scenario")
        assert dict(run2.meta) == {"mstr": "foo"}

    def test_update_meta(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        with run.transact("Update meta data"):
            run.meta = {"mnew": "bar"}

        assert dict(run.meta) == {"mnew": "bar"}

        run2 = platform.runs.get("Model", "Scenario")
        assert dict(run2.meta) == {"mnew": "bar"}

    def test_tabulate_platform_meta_after_update(
        self, platform: ixmp4.Platform
    ) -> None:
        exp = pd.DataFrame(
            [
                ["Model", "Scenario", 1, "mnew", "bar"],
            ],
            columns=["model", "scenario", "version", "key", "value"],
        )
        ret = platform.meta.tabulate(run__id=1)
        pdt.assert_frame_equal(exp, ret, check_like=True)

    def test_clear_meta(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        with run.transact("Clear meta data"):
            run.meta = {}
        assert dict(run.meta) == {}

        run2 = platform.runs.get("Model", "Scenario")
        assert dict(run2.meta) == {}

    def test_tabulate_platform_meta_after_delete(
        self, platform: ixmp4.Platform
    ) -> None:
        exp = pd.DataFrame(
            [],
            columns=["model", "scenario", "version", "key", "value"],
        )
        ret = platform.meta.tabulate(run__id=1)
        # index_type differs on http platforms here (integer v empty)
        pdt.assert_frame_equal(exp, ret, check_like=True, check_index_type=False)

    def test_two_runs_with_numpy_values(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        run1 = run
        run2 = platform.runs.create("Model 2", "Scenario 2")

        with run1.transact("Set meta data on both"):
            run1.meta = {"mnpint": np.float64(12)}
        with run2.transact("Set meta data on both"):
            run2.meta = {"mnpfloat": np.float64(3.1415926535897)}

    def test_tabulate_platform_meta_two_runs(self, platform: ixmp4.Platform) -> None:
        exp = pd.DataFrame(
            [
                ["Model", "Scenario", 1, "mnpint", 12],
                ["Model 2", "Scenario 2", 1, "mnpfloat", 3.1415926535897],
            ],
            columns=["model", "scenario", "version", "key", "value"],
        )
        ret = platform.meta.tabulate(run={"default_only": False})
        pdt.assert_frame_equal(exp, ret, check_like=True)

        exp_non_default = pd.DataFrame(
            [
                ["Model 2", "Scenario 2", 1, "mnpfloat", 3.1415926535897],
            ],
            columns=["model", "scenario", "version", "key", "value"],
        )
        ret_non_default = platform.meta.tabulate(run={"is_default": False})
        pdt.assert_frame_equal(exp_non_default, ret_non_default, check_like=True)


class TestMetaRunLock(MetaTest):
    def test_meta_requires_lock(self, run: ixmp4.Run) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.meta["mint"] = 13

        with pytest.raises(ixmp4.Run.LockRequired):
            run.meta = {"mint": 13, "mfloat": 0.0, "mstr": "foo"}

        with run.transact("Add meta data"):
            run.meta = {"mint": 13, "mfloat": 0.0, "mstr": "foo"}

        with pytest.raises(ixmp4.Run.LockRequired):
            del run.meta["mfloat"]
            run.meta["mstr"] = None


class TestMetaRollback(MetaTest):
    def test_meta_update_failure(self, run: ixmp4.Run) -> None:
        with run.transact("Add meta data"):
            run.meta = {"mint": 13, "mfloat": 0.0, "mstr": "foo"}
            run.meta["mfloat"] = -1.9

        try:
            with run.transact("Update meta data failure"):
                run.meta["mfloat"] = 3.14
                raise CustomException
        except CustomException:
            pass

    def test_meta_versioning_after_update_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        assert run.meta == {"mint": 13, "mfloat": -1.9, "mstr": "foo"}
        assert run.meta["mfloat"] == -1.9

    def test_meta_non_versioning_after_update_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        assert run.meta == {"mint": 13, "mfloat": 3.14, "mstr": "foo"}
        assert run.meta["mfloat"] == 3.14

    def test_meta_second_update_failure(self, run: ixmp4.Run) -> None:
        with run.transact("Remove meta data"):
            del run.meta["mfloat"]
            run.meta["mint"] = None

        try:
            with run.transact("Update meta data second failure"):
                run.meta["mfloat"] = 3.14
                raise CustomException
        except CustomException:
            pass

    def test_meta_versioning_after_second_update_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        assert run.meta == {"mstr": "foo"}
        assert run.meta["mstr"] == "foo"

    def test_meta_non_versioning_after_second_update_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        assert run.meta == {"mstr": "foo", "mfloat": 3.14}
        assert run.meta["mstr"] == "foo"
        assert run.meta["mfloat"] == 3.14
