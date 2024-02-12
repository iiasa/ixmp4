import contextlib
import cProfile
import pstats
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy.exc import OperationalError

from ixmp4 import Platform
from ixmp4.data.backend import RestTestBackend, SqliteTestBackend
from ixmp4.data.backend.db import PostgresTestBackend
from ixmp4.data.generator import MockDataGenerator

from .utils import gen_obj_nums

TEST_DATA_BIG = None
try:
    TEST_DATA_BIG = pd.read_csv("./tests/test-data/iamc-test-data_annual_big.csv")
    if TEST_DATA_BIG.empty:
        TEST_DATA_BIG = None
    # TEST_DATA_BIG = read_test_data(
    #     "./tests/test-data/very-big-test-data.xlsx"
    # ).reset_index(drop=True)
except FileNotFoundError:
    TEST_DATA_BIG = None  # skip benchmark tests

SKIP_PGSQL_TESTS = False
try:
    mp = Platform(_backend=PostgresTestBackend())
    mp.backend.close()
except OperationalError:
    SKIP_PGSQL_TESTS = True


@pytest.fixture(scope="function")
def test_data_big():
    yield TEST_DATA_BIG.copy()


TEST_DF_DATETIME = pd.DataFrame(
    [
        ["World", "Primary Energy", "EJ/yr", datetime(2005, 1, 1, 1, 0, 0), 1],
        ["World", "Primary Energy", "EJ/yr", datetime(2010, 1, 1, 1, 0, 0), 6.0],
        ["World", "Primary Energy|Coal", "EJ/yr", datetime(2005, 1, 1, 1, 0, 0), 0.5],
        ["World", "Primary Energy|Coal", "EJ/yr", datetime(2010, 1, 1, 1, 0, 0), 3],
    ],
    columns=["region", "variable", "unit", "step_datetime", "value"],
)


@pytest.fixture(scope="function")
def test_data_datetime():
    return TEST_DF_DATETIME.copy()


TEST_DF_CATEGORICAL = pd.DataFrame(
    [
        ["World", "Primary Energy", "EJ/yr", 2010, "A", 6.0],
        ["World", "Primary Energy", "EJ/yr", 2010, "B", 3],
        ["World", "Primary Energy", "EJ/yr", 2005, "A", 1],
        ["World", "Primary Energy", "EJ/yr", 2005, "B", 0.5],
    ],
    columns=["region", "variable", "unit", "step_year", "step_category", "value"],
)


@pytest.fixture(scope="function")
def test_data_categorical():
    df = TEST_DF_CATEGORICAL.copy()
    return df


TEST_DF_ANNUAL = pd.DataFrame(
    [
        ["World", "Primary Energy", "EJ/yr", 2005, 1],
        ["World", "Primary Energy", "EJ/yr", 2010, 6.0],
        ["World", "Primary Energy|Coal", "EJ/yr", 2005, 0.5],
        ["World", "Primary Energy|Coal", "EJ/yr", 2010, 3],
    ],
    columns=["region", "variable", "unit", "step_year", "value"],
)


@pytest.fixture(scope="function")
def test_data_annual():
    df = TEST_DF_ANNUAL.copy()
    return df


@pytest.fixture(scope="function")
def profiled(request):
    testname = request.node.name
    pr = cProfile.Profile()

    @contextlib.contextmanager
    def profiled():
        pr.enable()
        yield
        pr.disable()

    yield profiled
    ps = pstats.Stats(pr)
    Path(".profiles").mkdir(parents=True, exist_ok=True)
    ps.dump_stats(f".profiles/{testname}.prof")


@pytest.fixture
def test_sqlite_mp():
    return Platform(_backend=SqliteTestBackend())


@pytest.fixture
def test_pgsql_mp():
    mp = Platform(_backend=PostgresTestBackend())
    yield mp
    mp.backend.close()


@pytest.fixture
def test_api_sqlite_mp(test_sqlite_mp):
    return Platform(_backend=RestTestBackend(test_sqlite_mp.backend))


@pytest.fixture
def test_api_pgsql_mp(test_pgsql_mp):
    return Platform(_backend=RestTestBackend(test_pgsql_mp.backend))


@pytest.fixture(scope="module")
def test_sqlite_mp_generated():
    mp = Platform(_backend=SqliteTestBackend())
    generate_mock_data(mp)
    return mp


@pytest.fixture(scope="module")
def test_pgsql_mp_generated(request):
    mp = Platform(_backend=PostgresTestBackend())
    generate_mock_data(mp)
    yield mp
    mp.backend.close()


@pytest.fixture(scope="module")
def test_api_sqlite_mp_generated(test_sqlite_mp_generated):
    return Platform(_backend=RestTestBackend(test_sqlite_mp_generated.backend))


@pytest.fixture(scope="module")
def test_api_pgsql_mp_generated(test_pgsql_mp_generated):
    return Platform(_backend=RestTestBackend(test_pgsql_mp_generated.backend))


def generate_mock_data(mp):
    gen = MockDataGenerator(mp, **gen_obj_nums)
    gen.generate()
