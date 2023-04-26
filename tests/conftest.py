from pathlib import Path
from datetime import datetime
import contextlib
import cProfile
import pstats
import pandas as pd
import pytest

from sqlalchemy.exc import OperationalError, DatabaseError
from ixmp4 import Platform
from ixmp4.data.backend.db import OracleTestBackend, PostgresTestBackend
from ixmp4.data.abstract import DataPoint
from ixmp4.data.backend import SqliteTestBackend, RestTestBackend


def read_test_data(path):
    df = pd.read_excel(path)
    df = df.melt(
        id_vars=["model", "scenario", "region", "variable", "unit"],
        var_name="step_year",
        value_name="value",
    ).dropna(subset=["value"])
    df["step_year"] = df["step_year"].astype(int)
    df["type"] = DataPoint.Type.ANNUAL
    return df


try:
    TEST_DATA_BIG = read_test_data("./tests/test-data/big-test-data.xlsx").reset_index(
        drop=True
    )
    # TEST_DATA_BIG = read_test_data(
    #     "./tests/test-data/very-big-test-data.xlsx"
    # ).reset_index(drop=True)
except FileNotFoundError:
    TEST_DATA_BIG = None  # skip benchmark tests


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
    try:
        mp = Platform(_backend=PostgresTestBackend())
    except OperationalError as e:
        pytest.skip(
            f"Cannot connect to PostgreSQL database service, skipping test: {e}"
        )

    yield mp
    mp.backend.close()


@pytest.fixture
def test_oracle_mp():
    try:
        mp = Platform(_backend=OracleTestBackend())
    except DatabaseError as e:
        pytest.skip(f"Cannot connect to Oracle database service, skipping test: {e}")

    yield mp
    mp.backend.close()


@pytest.fixture
def test_api_mp():
    return Platform(_backend=RestTestBackend())
