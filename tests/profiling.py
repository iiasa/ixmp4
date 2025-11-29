import cProfile
import pstats
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ContextManager, TypeAlias

import pytest

ProfiledContextManager: TypeAlias = Callable[[], ContextManager[None]]


@pytest.fixture(scope="function")
def profiled(
    request: pytest.FixtureRequest,
) -> Generator[Callable[[], ProfiledContextManager]]:
    """Use this fixture for profiling tests:
    ```
    def test(profiled):
        # setup() ...
        with profiled():
            complex_procedure()
        # teardown() ...
    ```
    Profiler output will be written to '.profiles/{testname}.prof'
    """

    testname = request.node.name
    pr = cProfile.Profile()

    @contextmanager
    def profiled() -> Generator[None, Any, None]:
        pr.enable()
        yield
        pr.disable()

    yield profiled
    ps = pstats.Stats(pr)
    Path(".profiles").mkdir(parents=True, exist_ok=True)
    ps.dump_stats(f".profiles/{testname}.prof")
