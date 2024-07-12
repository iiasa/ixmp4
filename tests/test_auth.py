from typing import cast

import pandas as pd
import pytest

import ixmp4
from ixmp4.conf.auth import ManagerAuth
from ixmp4.conf.manager import ManagerPlatformInfo, MockManagerConfig
from ixmp4.conf.user import User
from ixmp4.core.exceptions import Forbidden, InvalidCredentials
from ixmp4.data.backend import SqlAlchemyBackend

from .fixtures import SmallIamcDataset


class TestAuthContext:
    small = SmallIamcDataset()
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

    mock_manager = MockManagerConfig(TEST_PLATFORMS, TEST_PERMISSIONS)

    @pytest.mark.parametrize(
        "user, truths",
        [
            (
                User(username="Superuser Sarah", is_superuser=True, is_verified=True),
                {
                    "ixmp4-public": dict(
                        access=True, manage=True, edit=True, view=True
                    ),
                    "ixmp4-gated": dict(access=True, manage=True, edit=True, view=True),
                    "ixmp4-private": dict(
                        access=True, manage=True, edit=True, view=True
                    ),
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
                    "ixmp4-public": dict(
                        access=True, manage=True, edit=True, view=True
                    ),
                    "ixmp4-gated": dict(access=True, manage=True, edit=True, view=True),
                    "ixmp4-private": dict(
                        access=False, manage=False, edit=False, view=False
                    ),
                },
            ),
            (
                User(
                    username="Staffuser Bob",
                    is_staff=True,
                    is_verified=True,
                    groups=[3, 5],
                ),
                {
                    "ixmp4-public": dict(
                        access=True, manage=False, edit=False, view=True
                    ),
                    "ixmp4-gated": dict(access=True, manage=True, edit=True, view=True),
                    "ixmp4-private": dict(
                        access=True, manage=True, edit=True, view=True
                    ),
                },
            ),
            (
                User(username="User Carina", is_verified=True, groups=[6, 7]),
                {
                    "ixmp4-public": dict(
                        access=True, manage=False, edit=True, view=True
                    ),
                    "ixmp4-gated": dict(
                        access=True, manage=False, edit=True, view=True
                    ),
                    "ixmp4-private": dict(
                        access=True, manage=False, edit=True, view=True
                    ),
                },
            ),
            (
                User(username="User Dave", is_verified=True, groups=[8]),
                {
                    "ixmp4-public": dict(
                        access=True, manage=False, edit=False, view=True
                    ),
                    "ixmp4-gated": dict(
                        access=True, manage=False, edit=True, view=True
                    ),
                    "ixmp4-private": dict(
                        access=False, manage=False, edit=False, view=False
                    ),
                },
            ),
        ],
    )
    def test_guards(self, sqlite_platform: ixmp4.Platform, user, truths):
        mp = sqlite_platform
        backend = cast(SqlAlchemyBackend, mp.backend)
        self.small.load_dataset(mp)

        for info in self.mock_manager.list_platforms():
            prm = truths[info.name]
            access, manage, edit, view = (
                prm["access"],
                prm["manage"],
                prm["edit"],
                prm["view"],
            )
            with backend.auth(user, self.mock_manager, info) as auth:
                assert auth.is_accessible == access
                assert (auth.is_managed or auth.user.is_superuser) == manage
                assert auth.is_editable == edit
                assert auth.is_viewable == view

                if not view:
                    with pytest.raises(Forbidden):
                        mp.models.list()
                    with pytest.raises(Forbidden):
                        mp.models.get("Does not exist")
                    with pytest.raises(Forbidden):
                        mp.models.get("Model 1")

                    with pytest.raises(Forbidden):
                        mp.scenarios.list()
                    with pytest.raises(Forbidden):
                        mp.scenarios.get("Does not exist")
                    with pytest.raises(Forbidden):
                        mp.scenarios.get("Scenario 1")

                    with pytest.raises(Forbidden):
                        mp.regions.list()
                    with pytest.raises(Forbidden):
                        mp.regions.get("Does not exist")
                    with pytest.raises(Forbidden):
                        mp.regions.get("Region 1")

                    with pytest.raises(Forbidden):
                        mp.units.list()
                    with pytest.raises(Forbidden):
                        mp.units.get("Does not exist")
                    with pytest.raises(Forbidden):
                        mp.units.get("Unit 1")

                if not manage:
                    with pytest.raises(Forbidden):
                        mp.regions.create("Created Region", hierarchy="default")
                    with pytest.raises(Forbidden):
                        mp.units.create("Created Unit")

                if view and not edit:
                    r = mp.regions.get("Region 1")
                    with pytest.raises(Forbidden):
                        r.docs = "Test Doc"
                    with pytest.raises(Forbidden):
                        del r.docs

                    u = mp.units.get("Unit 1")
                    with pytest.raises(Forbidden):
                        u.docs = "Test Doc"
                    with pytest.raises(Forbidden):
                        del u.docs

                    with pytest.raises(Forbidden):
                        mp.runs.create("Model 1", "Scenario 1")

                    run = mp.runs.get("Model 1", "Scenario 1")

                    with pytest.raises(Forbidden):
                        run.iamc.add(
                            self.small.datapoints.copy(),
                            type=ixmp4.DataPoint.Type.ANNUAL,
                        )

                    with pytest.raises(Forbidden):
                        run.iamc.remove(
                            self.small.datapoints.copy().drop(columns=["value"])
                        )

                    with pytest.raises(Forbidden):
                        run.meta = {"meta": "test"}

    @pytest.mark.parametrize(
        "model, platform_info, access",
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
    def test_filters(
        self,
        db_platform: ixmp4.Platform,
        model,
        platform_info,
        access,
    ):
        mp = db_platform
        backend = cast(SqlAlchemyBackend, mp.backend)
        user = User(username="User Carina", is_verified=True, groups=[6, 7])

        self.small.load_regions(mp)
        self.small.load_units(mp)

        run = mp.runs.create(model, "Scenario")
        annual_dps = self.small.datapoints.copy()
        run.iamc.add(annual_dps, type=ixmp4.DataPoint.Type.ANNUAL)
        run.meta = {"meta": "test"}
        run.set_as_default()

        with backend.auth(user, self.mock_manager, platform_info):
            if access in ["view", "edit"]:
                run = mp.runs.get(model, "Scenario")
                assert not run.iamc.tabulate().empty
                assert run.meta == {"meta": "test"}
                assert mp.models.list()[0].name == model

                if access == "edit":
                    run.iamc.add(annual_dps, type=ixmp4.DataPoint.Type.ANNUAL)
                    run.iamc.remove(
                        annual_dps.drop(columns=["value"]),
                        type=ixmp4.DataPoint.Type.ANNUAL,
                    )
                    run.meta = {"meta": "test"}

                else:
                    with pytest.raises(Forbidden):
                        _ = mp.runs.create(model, "Scenario")

                    with pytest.raises(Forbidden):
                        run.iamc.add(annual_dps, type=ixmp4.DataPoint.Type.ANNUAL)

                    with pytest.raises(Forbidden):
                        run.iamc.remove(
                            annual_dps.drop(columns=["value"]),
                            type=ixmp4.DataPoint.Type.ANNUAL,
                        )

                    with pytest.raises(Forbidden):
                        run.meta = {"meta": "test"}
            else:
                with pytest.raises((ixmp4.Run.NotFound, ixmp4.Run.NoDefaultVersion)):
                    mp.runs.get(model, "Scenario")

                assert mp.runs.tabulate().empty
                assert mp.runs.tabulate(default_only=False).empty
                assert mp.models.tabulate().empty
                assert mp.scenarios.tabulate().empty


def test_invalid_credentials():
    # TODO: Use testing instance once available.
    # Using dev for now to reduce load on production environment.
    # @wronguser cannot exist ("@" is not allowed) and will therefore always be invalid.
    with pytest.raises(InvalidCredentials):
        ManagerAuth(
            "@wronguser", "wrongpwd", "https://api.dev.manager.ece.iiasa.ac.at/v1/"
        )
