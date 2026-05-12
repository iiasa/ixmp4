from typing import Any

from ixmp4.data.compat_controller import EnumerationCompatibilityController


class RunCompatibilityController(EnumerationCompatibilityController):
    path = "/"

    def _get_compat_payload(
        self, query_params: dict[str, Any], body: bytes
    ) -> dict[str, Any]:
        payload = super()._get_compat_payload(query_params, body)
        table_arg = query_params.get("table", "false").lower()
        table_bool = table_arg == "true"
        if table_bool:
            payload.setdefault("include_internal_columns", True)
        return payload
