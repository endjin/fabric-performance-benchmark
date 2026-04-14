import base64
import json
import logging

from ._client import FABRIC_API_BASE, FabricRestClient

logger = logging.getLogger(__name__)


class FabricVariableLibrary:
    """Read and update Fabric Variable Library items via the REST API."""

    def __init__(self, client: FabricRestClient, workspace_id: str):
        self._client = client
        self._workspace_id = workspace_id

    def find_library(self, library_name: str) -> str:
        """Return the item GUID of the named variable library."""
        url = (
            f"{FABRIC_API_BASE}/v1/workspaces/{self._workspace_id}"
            f"/items?type=VariableLibrary"
        )
        logger.info("Looking up variable library '%s'", library_name)

        response = self._client.get(url)
        response.raise_for_status()

        for item in response.json().get("value", []):
            if item["displayName"] == library_name:
                library_id = item["id"]
                logger.info("Found variable library '%s' → %s", library_name, library_id)
                return library_id

        raise ValueError(
            f"Variable library '{library_name}' not found in workspace {self._workspace_id}"
        )

    def get_variables(self, library_id: str) -> tuple[list[dict], dict]:
        """Fetch the variable library definition and decode ``variables.json``.

        Returns ``(definition_parts, variables_payload)`` where
        *definition_parts* is the raw list of parts and *variables_payload*
        is the decoded JSON dict from ``variables.json``.
        """
        url = (
            f"{FABRIC_API_BASE}/v1/workspaces/{self._workspace_id}"
            f"/variableLibraries/{library_id}/getDefinition"
        )
        logger.info("Fetching definition for variable library %s", library_id)

        response = self._client.post(url)
        definition_response = self._client.handle_lro_response(response)

        parts = definition_response.json()["definition"]["parts"]
        variables_part = next(p for p in parts if p["path"] == "variables.json")
        variables_payload = json.loads(
            base64.b64decode(variables_part["payload"]).decode("utf-8")
        )

        logger.info(
            "Decoded variables.json — %d variables found",
            len(variables_payload.get("variables", [])),
        )
        return parts, variables_payload

    def update_variable(
        self,
        library_id: str,
        variable_name: str,
        new_value: str,
    ) -> None:
        """Read the variable library, update a single variable, and write it back."""
        parts, variables_payload = self.get_variables(library_id)

        # Find and update the target variable
        variable_found = False
        for var in variables_payload["variables"]:
            if var["name"] == variable_name:
                old_value = var["value"]
                var["value"] = new_value
                variable_found = True
                logger.info(
                    "Variable '%s' updated: '%s' → '%s'",
                    variable_name,
                    old_value,
                    new_value,
                )
                break

        if not variable_found:
            raise ValueError(
                f"Variable '{variable_name}' not found in variable library {library_id}"
            )

        # Re-encode and rebuild the definition
        updated_b64 = base64.b64encode(
            json.dumps(variables_payload, indent=2).encode("utf-8")
        ).decode("utf-8")

        updated_parts = []
        for part in parts:
            if part["path"] == "variables.json":
                updated_parts.append({
                    "path": "variables.json",
                    "payload": updated_b64,
                    "payloadType": "InlineBase64",
                })
            else:
                updated_parts.append(part)

        # Push update
        url = (
            f"{FABRIC_API_BASE}/v1/workspaces/{self._workspace_id}"
            f"/variableLibraries/{library_id}/updateDefinition"
        )
        logger.info("Updating definition for variable library %s", library_id)

        response = self._client.post(url, json_body={"definition": {"parts": updated_parts}})
        self._client.handle_lro_response(response)

        logger.info(
            "Variable library updated successfully — '%s' = '%s'",
            variable_name,
            new_value,
        )
