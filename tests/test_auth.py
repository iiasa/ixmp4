from typing import cast

import pandas as pd
import pytest

import ixmp4
from ixmp4.conf.auth import ManagerAuth
from ixmp4.conf.manager import ManagerPlatformInfo, MockManagerConfig
from ixmp4.conf.user import User
from ixmp4.core.exceptions import Forbidden, InvalidCredentials
from ixmp4.data.backend import SqlAlchemyBackend

from .core.test_run import assert_cloned_run
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
    def test_guards(
        self,
        sqlite_platform: ixmp4.Platform,
        user: User,
        truths: dict[str, dict[str, bool]],
    ) -> None:
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

                if view and not manage:
                    try:
                        run = mp.runs.get("Model 1", "Scenario 1")
                    except (ixmp4.Run.NotFound, ixmp4.Run.NoDefaultVersion):
                        pass  # cant view the run :()
                    else:
                        with pytest.raises(Forbidden):
                            with run.transact("Delete run"):
                                run.delete()

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
                        with run.transact("Add iamc data"):
                            run.iamc.add(
                                self.small.annual.copy(),
                                type=ixmp4.DataPoint.Type.ANNUAL,
                            )

                    with pytest.raises(Forbidden):
                        with run.transact("Remove iamc data"):
                            run.iamc.remove(
                                self.small.annual.copy().drop(columns=["value"])
                            )

                    with pytest.raises(Forbidden):
                        with run.transact("Add meta data"):
                            # NOTE mypy doesn't support setters taking a different
                            # type than their property
                            # https://github.com/python/mypy/issues/3004
                            run.meta = {"meta": "test"}  # type: ignore[assignment]

                    with pytest.raises(Forbidden):
                        _ = run.clone()

    def test_run_audit_info(self, db_platform: ixmp4.Platform) -> None:
        backend = cast(SqlAlchemyBackend, db_platform.backend)

        test_user = User(username="test_audit", is_verified=True, is_superuser=True)

        run1 = backend.runs.create("Model 1", "Scenario 1")

        backend.runs.create("Model 1", "Scenario 1")
        backend.runs.set_as_default_version(run1.id)

        with backend.auth(test_user, self.mock_manager, self.TEST_PLATFORMS[0]):
            run3 = backend.runs.create("Model 1", "Scenario 1")
            backend.runs.set_as_default_version(run3.id)

        runs = backend.runs.tabulate(default_only=False)
        assert runs["created_by"][0] == "@unknown"
        assert runs["created_by"][1] == "@unknown"
        assert runs["created_by"][2] == "test_audit"

        # run1 was updated by set_as_default_version
        # run2 was not
        assert runs["updated_by"][0] == "@unknown"
        assert runs["updated_by"][1] is None
        assert runs["updated_by"][2] == "test_audit"

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
        model: str,
        platform_info: ManagerPlatformInfo,
        access: str | None,
    ) -> None:
        mp = db_platform
        backend = cast(SqlAlchemyBackend, mp.backend)
        user = User(username="User Carina", is_verified=True, groups=[6, 7])

        self.small.load_regions(mp)
        self.small.load_units(mp)

        run = mp.runs.create(model, "Scenario")
        annual_dps = self.small.annual.copy()
        with run.transact("Add iamc data"):
            run.iamc.add(annual_dps, type=ixmp4.DataPoint.Type.ANNUAL)
        with run.transact("Add meta data"):
            run.meta = {"meta": "test"}  # type: ignore[assignment]
        run.set_as_default()

        with backend.auth(user, self.mock_manager, platform_info):
            if access in ["view", "edit"]:
                run = mp.runs.get(model, "Scenario")
                assert not run.iamc.tabulate().empty
                assert run.meta == {"meta": "test"}
                assert mp.models.list()[0].name == model

                if access == "edit":
                    with run.transact("Add and remove iamc data"):
                        run.iamc.add(annual_dps, type=ixmp4.DataPoint.Type.ANNUAL)
                        run.iamc.remove(
                            annual_dps.drop(columns=["value"]),
                            type=ixmp4.DataPoint.Type.ANNUAL,
                        )
                    with run.transact("Add meta data"):
                        run.meta = {"meta": "test"}  # type: ignore[assignment]

                    # Test run.clone() uses auth()
                    clone_with_solution = run.clone()
                    assert_cloned_run(run, clone_with_solution, kept_solution=True)

                else:
                    with pytest.raises(Forbidden):
                        _ = mp.runs.create(model, "Scenario")

                    with pytest.raises(Forbidden):
                        with run.transact("Add iamc data"):
                            run.iamc.add(annual_dps, type=ixmp4.DataPoint.Type.ANNUAL)

                    with pytest.raises(Forbidden):
                        with run.transact("Remove iamc data"):
                            run.iamc.remove(
                                annual_dps.drop(columns=["value"]),
                                type=ixmp4.DataPoint.Type.ANNUAL,
                            )

                    with pytest.raises(Forbidden):
                        with run.transact("Add meta data"):
                            run.meta = {"meta": "test"}  # type: ignore[assignment]

                    with pytest.raises(Forbidden):
                        _ = run.clone()
            else:
                with pytest.raises((ixmp4.Run.NotFound, ixmp4.Run.NoDefaultVersion)):
                    mp.runs.get(model, "Scenario")

                assert mp.runs.tabulate().empty
                assert mp.runs.tabulate(default_only=False).empty
                assert mp.models.tabulate().empty
                assert mp.scenarios.tabulate().empty


def test_invalid_credentials() -> None:
    # TODO: Use testing instance once available.
    # Using dev for now to reduce load on production environment.
    # @wronguser cannot exist ("@" is not allowed) and will therefore always be invalid.
    with pytest.raises(InvalidCredentials):
        ManagerAuth(
            "@wronguser", "wrongpwd", "https://api.testing.manager.ece.iiasa.ac.at/v1/"
        )
