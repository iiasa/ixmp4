import pytest
import pandas as pd

import ixmp4
from ixmp4.core.exceptions import Forbidden
from ixmp4.conf.user import User
from ixmp4.conf.manager import ManagerPlatformInfo, MockManagerConfig

from .utils import add_regions, add_units, database_platforms

public = ManagerPlatformInfo(
    id=1,
    slug="ixmp4-public",
    dsn=":memory:",
    management_group=1,
    access_group=2,
    url="https://ixmp4-public",
    accessibility=ManagerPlatformInfo.Accessibilty.PUBLIC,
)
gated = ManagerPlatformInfo(
    id=2,
    slug="ixmp4-gated",
    dsn=":memory:",
    management_group=3,
    access_group=2,
    url="https://ixmp4-gated",
    accessibility=ManagerPlatformInfo.Accessibilty.GATED,
)
private = ManagerPlatformInfo(
    id=3,
    slug="ixmp4-private",
    dsn=":memory:",
    management_group=5,
    access_group=6,
    url="https://ixmp4-private",
    accessibility=ManagerPlatformInfo.Accessibilty.PRIVATE,
)

TEST_PLATFORMS = [public, gated, private]
TEST_PERMISSIONS = pd.DataFrame(
    [
        # Group 2 is the default access group
        [1, 1, 2, "VIEW", "*"],
        [2, 2, 2, "VIEW", "Model"],
        # Group 2 is the private access group
        [3, 3, 6, "VIEW", "Model"],
        # Group 7 edits all
        [4, 1, 7, "EDIT", "*"],
        [5, 2, 7, "EDIT", "Model"],
        [6, 3, 7, "EDIT", "Model 1*"],
        # Group 8 edits gated
        [7, 2, 8, "EDIT", "*"],
    ],
    columns=["id", "instance", "group", "access_type", "model"],
)

TEST_DF = pd.DataFrame(
    [
        ["Test Region", "Variable 1", "Test Unit", 2005, 1],
        ["Test Region", "Variable 1", "Test Unit", 2010, 6.0],
        ["Test Region", "Variable 2", "Test Unit", 2005, 0.5],
        ["Test Region", "Variable 2", "Test Unit", 2010, 3],
    ],
    columns=["region", "variable", "unit", "step_year", "value"],
)

mock_manager = MockManagerConfig(TEST_PLATFORMS, TEST_PERMISSIONS)


@pytest.mark.parametrize(
    "user, truths",
    [
        (
            User(username="Superuser Sarah", is_superuser=True, is_verified=True),
            {
                "ixmp4-public": dict(access=True, manage=True, edit=True, view=True),
                "ixmp4-gated": dict(access=True, manage=True, edit=True, view=True),
                "ixmp4-private": dict(access=True, manage=True, edit=True, view=True),
            },
        ),
        (
            User(
                username="Staffuser Alice",
                is_staff=True,
                is_verified=True,
                groups=[1, 3],
            ),
            {
                "ixmp4-public": dict(access=True, manage=True, edit=True, view=True),
                "ixmp4-gated": dict(access=True, manage=True, edit=True, view=True),
                "ixmp4-private": dict(
                    access=False, manage=False, edit=False, view=False
                ),
            },
        ),
        (
            User(
                username="Staffuser Bob", is_staff=True, is_verified=True, groups=[3, 5]
            ),
            {
                "ixmp4-public": dict(access=True, manage=False, edit=False, view=True),
                "ixmp4-gated": dict(access=True, manage=True, edit=True, view=True),
                "ixmp4-private": dict(access=True, manage=True, edit=True, view=True),
            },
        ),
        (
            User(username="User Carina", is_verified=True, groups=[6, 7]),
            {
                "ixmp4-public": dict(access=True, manage=False, edit=True, view=True),
                "ixmp4-gated": dict(access=True, manage=False, edit=True, view=True),
                "ixmp4-private": dict(access=True, manage=False, edit=True, view=True),
            },
        ),
        (
            User(username="User Dave", is_verified=True, groups=[8]),
            {
                "ixmp4-public": dict(access=True, manage=False, edit=False, view=True),
                "ixmp4-gated": dict(access=True, manage=False, edit=True, view=True),
                "ixmp4-private": dict(
                    access=False, manage=False, edit=False, view=False
                ),
            },
        ),
    ],
)
def test_guards(user, truths, test_sqlite_mp):
    mp = test_sqlite_mp
    mp.regions.create("Test Region", hierarchy="default")
    mp.units.create("Test Unit")

    run = mp.Run("Model", "Scenario", version="new")
    run.iamc.add(TEST_DF, type=ixmp4.DataPoint.Type.ANNUAL)
    run.set_as_default()

    for info in mock_manager.list_platforms():
        prm = truths[info.name]
        access, manage, edit, view = (
            prm["access"],
            prm["manage"],
            prm["edit"],
            prm["view"],
        )
        with mp.backend.auth(user, mock_manager, info) as auth:
            assert auth.is_accessible == access
            assert auth.is_managed == manage
            assert auth.is_editable == edit
            assert auth.is_viewable == view

            if not view:
                with pytest.raises(Forbidden):
                    mp.models.list()
                with pytest.raises(Forbidden):
                    mp.models.get("Test")

                with pytest.raises(Forbidden):
                    mp.scenarios.list()
                with pytest.raises(Forbidden):
                    mp.scenarios.get("Test")

                with pytest.raises(Forbidden):
                    mp.regions.list()
                with pytest.raises(Forbidden):
                    mp.regions.get("Test")

                with pytest.raises(Forbidden):
                    mp.units.list()
                with pytest.raises(Forbidden):
                    mp.units.get("Test")

            if not manage:
                with pytest.raises(Forbidden):
                    mp.regions.create("Test", hierarchy="default")
                with pytest.raises(Forbidden):
                    mp.units.create("Test")

            if view and not edit:
                r = mp.regions.get("Test Region")
                with pytest.raises(Forbidden):
                    r.docs = "Test Doc"
                with pytest.raises(Forbidden):
                    del r.docs

                u = mp.units.get("Test Unit")
                with pytest.raises(Forbidden):
                    u.docs = "Test Doc"
                with pytest.raises(Forbidden):
                    del u.docs

                with pytest.raises(Forbidden):
                    run = mp.Run("Model", "Scenario", version="new")

                run = mp.Run("Model", "Scenario")

                with pytest.raises(Forbidden):
                    run.iamc.add(TEST_DF, type=ixmp4.DataPoint.Type.ANNUAL)

                with pytest.raises(Forbidden):
                    run.iamc.remove(TEST_DF.drop(columns=["value"]))

                with pytest.raises(Forbidden):
                    run.meta = {"meta": "test"}


@database_platforms
@pytest.mark.parametrize(
    "model, platform, access",
    [
        ["Model", public, "edit"],
        ["Model", gated, "edit"],
        ["Model", private, "view"],
        ["Model 1", public, "edit"],
        ["Model 1", gated, None],
        ["Model 1", private, "edit"],
        ["Model 1.1", public, "edit"],
        ["Model 1.1", gated, None],
        ["Model 1.1", private, "edit"],
        ["Other Model", public, "edit"],
        ["Other Model", gated, None],
        ["Other Model", private, None],
    ],
)
def test_filters(model, platform, access, test_mp, test_data_annual):
    user = User(username="User Carina", is_verified=True, groups=[6, 7])

    mp = test_mp
    add_regions(mp, test_data_annual["region"].unique())
    add_units(mp, test_data_annual["unit"].unique())

    run = mp.Run(model, "Scenario", version="new")
    run.iamc.add(test_data_annual, type=ixmp4.DataPoint.Type.ANNUAL)
    run.meta = {"meta": "test"}
    run.set_as_default()

    with mp.backend.auth(user, mock_manager, platform):
        if access in ["view", "edit"]:
            run = mp.Run(model, "Scenario")
            assert not run.iamc.tabulate().empty
            assert run.meta == {"meta": "test"}
            assert mp.models.list()[0].name == model

            if access == "edit":
                run.iamc.add(test_data_annual, type=ixmp4.DataPoint.Type.ANNUAL)
                run.iamc.remove(
                    test_data_annual.drop(columns=["value"]),
                    type=ixmp4.DataPoint.Type.ANNUAL,
                )
                run.meta = {"meta": "test"}

            else:
                with pytest.raises(Forbidden):
                    run.iamc.add(test_data_annual, type=ixmp4.DataPoint.Type.ANNUAL)

                with pytest.raises(Forbidden):
                    run.iamc.remove(
                        test_data_annual.drop(columns=["value"]),
                        type=ixmp4.DataPoint.Type.ANNUAL,
                    )

                with pytest.raises(Forbidden):
                    run.meta = {"meta": "test"}
        else:
            with pytest.raises((ixmp4.Run.NotFound, ixmp4.Run.NoDefaultVersion)):
                mp.Run(model, "Scenario")

            assert mp.runs.tabulate().empty
            assert mp.runs.tabulate(default_only=False).empty
            assert mp.models.tabulate().empty
            assert mp.scenarios.tabulate().empty
