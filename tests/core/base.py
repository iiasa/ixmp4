from typing import TypeVar

import pytest

from ixmp4 import Platform
from ixmp4.backend import Backend
from tests.base import TransportTest

ServiceT = TypeVar("ServiceT")


class PlatformTest(TransportTest):
    @pytest.fixture(scope="class")
    def versioning_platform(self, platform: Platform) -> Platform:
        direct = self.get_direct_or_skip(platform.backend.transport)
        versioning_platform = Platform(_backend=Backend(direct))

        if self.transport_is_pgsql(direct):
            return versioning_platform
        else:
            self.skip_transport(
                platform.backend.transport, "does not support versioning"
            )

    @pytest.fixture(scope="class")
    def non_versioning_platform(self, platform: Platform) -> Platform:
        direct = self.get_direct_or_skip(platform.backend.transport)

        if not self.transport_is_pgsql(direct):
            return platform
        else:
            self.skip_transport(platform.backend.transport, "supports versioning")
