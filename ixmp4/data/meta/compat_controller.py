from typing import Any

from ixmp4.data.compat_controller import EnumerationCompatibilityController


class RunMetaEntryCompatibilityController(EnumerationCompatibilityController):
    path = "/"

    def _get_compat_payload(
        self, query_params: dict[str, Any], body: bytes
    ) -> dict[str, Any]:
        payload = super()._get_compat_payload(query_params, body)
        if "run_id" in payload:
            payload["run__id"] = payload.pop("run_id")
        return payload
