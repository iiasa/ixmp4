import threading
import time

import pytest

import ixmp4
from tests import backends

platform = backends.get_platform_fixture(scope="function")


class TestRunTransact:
    def test_transact_timeout(self, platform: ixmp4.Platform) -> None:
        _ = platform.runs.create("Model", "Scenario")
        run1 = platform.runs.get("Model", "Scenario", version=1)
        run2 = platform.runs.get("Model", "Scenario", version=1)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                sync_lock.release()
                time.sleep(0.1)

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        with run2.transact("Test transaction", timeout=2):
            run2.meta["mstr"] = "baz"

        assert run2.meta["mstr"] == "baz"
        thread.join()
        sync_lock.release()

    def test_transact_timeout_failure(self, platform: ixmp4.Platform) -> None:
        _ = platform.runs.create("Model", "Scenario")
        run1 = platform.runs.get("Model", "Scenario", version=1)
        run2 = platform.runs.get("Model", "Scenario", version=1)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                sync_lock.release()
                time.sleep(2)

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        with pytest.raises(ixmp4.Run.IsLocked):
            with run2.transact("Test transaction", timeout=0.5):
                run2.meta["mstr"] = "baz"

        assert run2.meta == {}
        thread.join()
        sync_lock.release()

    def test_transact_is_locked(self, platform: ixmp4.Platform) -> None:
        _ = platform.runs.create("Model", "Scenario")
        run1 = platform.runs.get("Model", "Scenario", version=1)
        run2 = platform.runs.get("Model", "Scenario", version=1)
        sync_lock = threading.Lock()
        sync_lock.acquire(timeout=1)

        def background_task() -> None:
            with run1.transact("Background transaction"):
                sync_lock.release()
                time.sleep(2)

        thread = threading.Thread(target=background_task)
        thread.start()

        sync_lock.acquire(timeout=1)

        with pytest.raises(ixmp4.Run.IsLocked):
            with run2.transact("Test transaction"):
                run2.meta["mstr"] = "baz"

        thread.join()
        sync_lock.release()
