import pytest
from toolkit.auth.context import AuthorizationContext
from toolkit.auth.user import User
from toolkit.manager.mock import MockManagerClient
from toolkit.manager.models import Ixmp4Instance


@pytest.fixture(scope="class")
def mock_manager_client() -> MockManagerClient:
    from .fixtures import get_manager_fixtures

    fixtures = get_manager_fixtures()
    return MockManagerClient(fixtures)


@pytest.fixture(scope="session")
def superuser_sarah() -> User | None:
    return User(
        id=1,
        username="superuser_sarah",
        email="sarah@nomail.ece.iiasa.ac.at",
        groups=[],
        is_superuser=True,
        is_staff=True,
        is_verified=True,
    )


class SarahTest:
    """Sarah has superuser permission and can thus do anything
    on any instance, even without being in any groups..."""

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, superuser_sarah: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(superuser_sarah, mock_manager_client)


@pytest.fixture(scope="session")
def staffuser_alice() -> User | None:
    return User(
        id=2,
        username="staffuser_alice",
        email="alice@nomail.ece.iiasa.ac.at",
        groups=[6, 7],
        is_superuser=False,
        is_staff=True,
        is_verified=True,
    )


class AliceTest:
    """Alice is in the management group for 'IXMP4 Dev Public' and 'IXMP4 Dev Gated'.

    'IXMP4 Dev Public':
        - Management
    'IXMP4 Dev Gated':
        - Management
    """

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, staffuser_alice: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(staffuser_alice, mock_manager_client)


@pytest.fixture(scope="session")
def staffuser_bob() -> User | None:
    return User(
        id=3,
        username="staffuser_bob",
        email="bob@nomail.ece.iiasa.ac.at",
        groups=[7, 8],
        is_superuser=False,
        is_staff=True,
        is_verified=True,
    )


class BobTest:
    """Bob is in the management group for 'IXMP4 Dev Gated' and 'IXMP4 Dev Private'.

    'IXMP4 Dev Gated':
        - Management
    'IXMP4 Dev Private':
        - Management
    """

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, staffuser_bob: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(staffuser_bob, mock_manager_client)


@pytest.fixture(scope="session")
def user_carina() -> User | None:
    return User(
        id=4,
        username="user_carina",
        email="carina@nomail.ece.iiasa.ac.at",
        groups=[3, 4],
        is_superuser=False,
        is_staff=False,
        is_verified=True,
    )


class CarinaTest:
    """Carina is in the access group for 'IXMP4 Dev Private'.

    Permissions:
    'IXMP Dev Public':
        - EDIT *
    'IXMP4 Dev Gated':
        - EDIT Model
    'IXMP4 Dev Private':
        - Access
        - VIEW Model
        - EDIT Model 1*
    """

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, user_carina: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(user_carina, mock_manager_client)


@pytest.fixture(scope="session")
def user_dave() -> User | None:
    return User(
        id=5,
        username="user_dave",
        email="dave@nomail.ece.iiasa.ac.at",
        groups=[5],
        is_superuser=False,
        is_staff=False,
        is_verified=True,
    )


class DaveTest:
    """Dave has no special access or management permissions.

    Permissions:
    'IXMP4 Dev Gated':
        - EDIT *
    """

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, user_dave: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(user_dave, mock_manager_client)


@pytest.fixture(scope="session")
def user_eve() -> User | None:
    return User(
        id=6,
        username="user_eve",
        email="eve@nomail.ece.iiasa.ac.at",
        groups=[],
        is_superuser=False,
        is_staff=False,
        is_verified=True,
    )


class EveTest:
    """Eve has no special access or management permissions
    and is not a member of any groups."""

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, user_eve: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(user_eve, mock_manager_client)


@pytest.fixture(scope="session")
def none_user() -> User | None:
    return None


class NoneTest:
    """Unauthenticated user."""

    @pytest.fixture(scope="class")
    def auth_ctx(
        self, none_user: User | None, mock_manager_client: MockManagerClient
    ) -> AuthorizationContext:
        return AuthorizationContext(none_user, mock_manager_client)


@pytest.fixture(scope="session")
def platform_public() -> Ixmp4Instance:
    return Ixmp4Instance(
        id=3,
        name="IXMP4 Dev Public",
        slug="dev-public",
        management_group=6,
        access_group=2,
        accessibility="PUBLIC",
        url="https://dev.ixmp.ece.iiasa.ac.at/v1/dev-public",
        dsn="sqlite:///{env:IXMP4_DIR}/databases/dev-public.sqlite3",
        workflow_repository="git@github.com:iiasa/scse-processing-test-workflow",
    )


class PublicPlatformTest:
    @pytest.fixture(scope="class")
    def platform_info(self, platform_public: Ixmp4Instance) -> Ixmp4Instance:
        return platform_public


@pytest.fixture(scope="session")
def platform_gated() -> Ixmp4Instance:
    return Ixmp4Instance(
        id=4,
        name="IXMP4 Dev Gated",
        slug="dev-gated",
        management_group=7,
        access_group=2,
        accessibility="GATED",
        url="https://dev.ixmp.ece.iiasa.ac.at/v1/dev-gated",
        dsn="sqlite:///{env:IXMP4_DIR}/databases/dev-gated.sqlite3",
        workflow_repository="git@github.com:iiasa/scse-processing-test-workflow",
    )


class GatedPlatformTest:
    @pytest.fixture(scope="class")
    def platform_info(self, platform_gated: Ixmp4Instance) -> Ixmp4Instance:
        return platform_gated


@pytest.fixture(scope="session")
def platform_private() -> Ixmp4Instance:
    return Ixmp4Instance(
        id=5,
        name="IXMP4 Dev Private",
        slug="dev-private",
        management_group=8,
        access_group=3,
        accessibility="PRIVATE",
        url="https://dev.ixmp.ece.iiasa.ac.at/v1/dev-private",
        dsn="sqlite:///{env:IXMP4_DIR}/databases/dev-private.sqlite3",
        workflow_repository="git@github.com:iiasa/scse-processing-test-workflow",
    )


class PrivatePlatformTest:
    @pytest.fixture(scope="class")
    def platform_info(self, platform_private: Ixmp4Instance) -> Ixmp4Instance:
        return platform_private
