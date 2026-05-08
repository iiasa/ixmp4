"""Boundary tests for the ``ixmp4.conf`` package.

Verifies that all public symbols intended for external consumers are
importable from the canonical sub-module paths and have the expected
structure.
"""


class TestSettingsImportable:
    def test_settings(self) -> None:
        from ixmp4.conf.settings import Settings

        assert isinstance(Settings, type)

    def test_client_settings(self) -> None:
        from ixmp4.conf.settings import ClientSettings

        assert isinstance(ClientSettings, type)

    def test_server_settings(self) -> None:
        from ixmp4.conf.settings import ServerSettings

        assert isinstance(ServerSettings, type)


class TestCredentialsImportable:
    def test_credentials(self) -> None:
        from ixmp4.conf.credentials import Credentials

        assert isinstance(Credentials, type)

    def test_credentials_dict(self) -> None:
        from ixmp4.conf.credentials import CredentialsDict

        instance: CredentialsDict = {"username": "u", "password": "p"}
        assert isinstance(instance, dict)


class TestPlatformsImportable:
    def test_platform_connection_info(self) -> None:
        from ixmp4.conf.platforms import PlatformConnectionInfo

        assert callable(PlatformConnectionInfo)

    def test_platform_connections(self) -> None:
        from ixmp4.conf.platforms import PlatformConnections

        assert isinstance(PlatformConnections, type)

    def test_toml_platform(self) -> None:
        from ixmp4.conf.platforms import TomlPlatform

        assert isinstance(TomlPlatform, type)

    def test_toml_platforms(self) -> None:
        from ixmp4.conf.platforms import TomlPlatforms

        assert isinstance(TomlPlatforms, type)

    def test_manager_platforms(self) -> None:
        from ixmp4.conf.platforms import ManagerPlatforms

        assert isinstance(ManagerPlatforms, type)

    def test_resolve_dsn_env_tokens(self) -> None:
        from ixmp4.conf.platforms import resolve_dsn_env_tokens

        assert callable(resolve_dsn_env_tokens)
