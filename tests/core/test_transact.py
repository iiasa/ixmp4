import threading
import time

import pytest

import ixmp4
from ixmp4.core.exceptions import RunIsLocked


class TestCoreTransact:
    def test_transact_timeout(self, platform: ixmp4.Platform) -> None:
        _ = platform.runs.create("Model", "Scenario")
        run1 = platform.runs.get("Model", "Scenario", version=1)
        run2 = platform.runs.get("Model", "Scenario", version=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                time.sleep(0.2)

        thread = threading.Thread(target=background_task)
        thread.start()

        time.sleep(0.1)

        with run2.transact("Test transaction", timeout=2):
            run2.meta["mstr"] = "baz"

        assert run2.meta["mstr"] == "baz"
        thread.join()

    def test_transact_timeout_failure(self, platform: ixmp4.Platform) -> None:
        _ = platform.runs.create("Model", "Scenario")
        run1 = platform.runs.get("Model", "Scenario", version=1)
        run2 = platform.runs.get("Model", "Scenario", version=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                time.sleep(1)

        thread = threading.Thread(target=background_task)
        thread.start()

        time.sleep(0.1)

        with pytest.raises(RunIsLocked):
            with run2.transact("Test transaction", timeout=0.5):
                run2.meta["mstr"] = "baz"

        assert run2.meta == {}
        thread.join()

    def test_transact_is_locked(self, platform: ixmp4.Platform) -> None:
        _ = platform.runs.create("Model", "Scenario")
        run1 = platform.runs.get("Model", "Scenario", version=1)
        run2 = platform.runs.get("Model", "Scenario", version=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                time.sleep(1)

        thread = threading.Thread(target=background_task)
        thread.start()

        time.sleep(0.1)

        with pytest.raises(RunIsLocked):
            with run2.transact("Test transaction"):
                run2.meta["mstr"] = "baz"

        thread.join()
