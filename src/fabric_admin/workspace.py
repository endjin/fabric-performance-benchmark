import base64
import json
import logging
import re

from ._client import FABRIC_API_BASE, FabricRestClient

logger = logging.getLogger(__name__)


class FabricWorkspace:
    """Operations against a single Fabric workspace identified by name."""

    def __init__(self, client: FabricRestClient, workspace_name: str):
        self._client = client
        self.workspace_name = workspace_name
        self._workspace_id: str | None = None

    @property
    def workspace_id(self) -> str:
        if self._workspace_id is None:
            self._workspace_id = self.resolve_workspace_id()
        return self._workspace_id

    def resolve_workspace_id(self) -> str:
        """Resolve a workspace GUID from its display name.

        Calls ``GET /v1/workspaces`` and matches by ``displayName``.
        """
        logger.info("Resolving workspace ID for '%s'", self.workspace_name)
        url = f"{FABRIC_API_BASE}/v1/workspaces"

        while url:
            response = self._client.get(url)
            response.raise_for_status()
            body = response.json()

            for ws in body.get("value", []):
                if ws["displayName"] == self.workspace_name:
                    ws_id = ws["id"]
                    logger.info("Resolved workspace '%s' → %s", self.workspace_name, ws_id)
                    return ws_id

            continuation = body.get("continuationUri")
            url = continuation if continuation else None

        raise ValueError(f"Workspace '{self.workspace_name}' not found")

    def provision_identity(self) -> dict | None:
        """Provision a workspace identity (managed service principal).

        Returns the identity dict (``applicationId``, ``servicePrincipalId``)
        on success, or ``None`` if the identity already existed.
        """
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/provisionIdentity"
        logger.info("Provisioning workspace identity for workspace %s", self.workspace_id)

        response = self._client.post(url)

        if response.status_code == 200:
            identity = response.json()
            logger.info(
                "Workspace identity provisioned — applicationId=%s, servicePrincipalId=%s",
                identity.get("applicationId"),
                identity.get("servicePrincipalId"),
            )
            return identity

        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise RuntimeError("202 response missing Location header for LRO polling")
            logger.info("Workspace identity provisioning accepted — polling LRO")
            operation_response = self._client.poll_long_running_operation(location)

            result_location = operation_response.headers.get("Location")
            if result_location:
                result = self._client.get(result_location)
                identity = result.json()
            else:
                identity = operation_response.json()

            logger.info(
                "Workspace identity provisioned — applicationId=%s, servicePrincipalId=%s",
                identity.get("applicationId"),
                identity.get("servicePrincipalId"),
            )
            return identity

        # Non-success — check for "already exists"
        error_body = response.json() if response.content else {}
        error_code = error_body.get("errorCode", "")
        message = error_body.get("message", "")

        if "already" in error_code.lower() or "already" in message.lower() or "exist" in message.lower():
            logger.info("Workspace identity already exists (HTTP %s)", response.status_code)
            return None

        raise RuntimeError(
            f"Failed to provision workspace identity (HTTP {response.status_code}): "
            f"{json.dumps(error_body, indent=2)}"
        )

    def list_role_assignments(self) -> list[dict]:
        """Return all role assignments for the workspace."""
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/roleAssignments"
        logger.debug("Listing role assignments for workspace %s", self.workspace_id)
        response = self._client.get(url)
        response.raise_for_status()
        assignments = response.json().get("value", [])
        logger.debug("Found %d role assignments", len(assignments))
        return assignments

    def add_role_assignment(
        self,
        principal_id: str,
        principal_type: str = "ServicePrincipal",
        role: str = "Contributor",
    ) -> None:
        """Add a role assignment if the principal does not already have one."""
        assignments = self.list_role_assignments()

        for assignment in assignments:
            principal = assignment.get("principal", {})
            if principal.get("id") == principal_id and principal.get("type") == principal_type:
                existing_role = assignment.get("role")
                logger.info(
                    "Principal %s already has role '%s' — skipping assignment",
                    principal_id,
                    existing_role,
                )
                return

        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/roleAssignments"
        payload = {
            "principal": {"id": principal_id, "type": principal_type},
            "role": role,
        }
        logger.info("Adding %s role for %s (%s)", role, principal_id, principal_type)
        response = self._client.post(url, json_body=payload)

        if response.status_code == 201:
            logger.info("Role assignment created successfully")
        else:
            error_body = response.json() if response.content else {}
            raise RuntimeError(
                f"Failed to create role assignment (HTTP {response.status_code}): "
                f"{json.dumps(error_body, indent=2)}"
            )

    def ensure_identity_with_contributor_access(self) -> dict | None:
        """Provision the workspace identity and ensure it has Contributor role.

        Returns the identity dict, or ``None`` if the identity already existed
        and the service principal ID could not be determined.
        """
        identity = self.provision_identity()

        if identity is None:
            logger.warning(
                "Workspace identity already existed — cannot determine service principal ID. "
                "Verify contributor access manually in workspace settings."
            )
            return None

        sp_id = identity.get("servicePrincipalId")
        if sp_id:
            self.add_role_assignment(sp_id, principal_type="ServicePrincipal", role="Contributor")
        else:
            logger.warning("No servicePrincipalId in identity response — cannot assign role")

        return identity

    def get_item_id(self, item_name: str, item_type: str) -> str:
        """Find an item GUID by display name and type within the workspace."""
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/items?type={item_type}"
        logger.info("Looking up %s '%s' in workspace %s", item_type, item_name, self.workspace_id)

        while url:
            response = self._client.get(url)
            response.raise_for_status()
            body = response.json()

            for item in body.get("value", []):
                if item["displayName"] == item_name:
                    item_id = item["id"]
                    logger.info("Found %s '%s' → %s", item_type, item_name, item_id)
                    return item_id

            url = body.get("continuationUri")

        raise ValueError(f"{item_type} '{item_name}' not found in workspace {self.workspace_id}")

    def get_lakehouse_sql_endpoint(self, lakehouse_name: str) -> tuple[str, str]:
        """Return the SQL endpoint (server, database_id) for a lakehouse.

        Calls ``GET /v1/workspaces/{id}/lakehouses/{id}`` and extracts the
        SQL analytics endpoint connection string and database ID.
        """
        lakehouse_id = self.get_item_id(lakehouse_name, item_type="Lakehouse")
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/lakehouses/{lakehouse_id}"
        logger.info("Fetching lakehouse properties for '%s' (%s)", lakehouse_name, lakehouse_id)

        response = self._client.get(url)
        response.raise_for_status()
        props = response.json().get("properties", {})
        sql_props = props.get("sqlEndpointProperties", {})

        connection_string = sql_props.get("connectionString", "")
        database_id = sql_props.get("id", "")

        if not connection_string or not database_id:
            raise RuntimeError(
                f"Lakehouse '{lakehouse_name}' SQL endpoint not available. "
                f"Provisioning status: {sql_props.get('provisioningStatus', 'unknown')}"
            )

        logger.info(
            "Lakehouse SQL endpoint: server=%s, database=%s",
            connection_string, database_id,
        )
        return connection_string, database_id

    def update_semantic_model_lakehouse_connection(
        self,
        semantic_model_name: str,
        sql_endpoint_server: str,
        sql_endpoint_database_id: str,
    ) -> None:
        """Rewrite the Sql.Database() connection in a semantic model's expressions.tmdl.

        Fetches the current TMDL definition, updates the ``Sql.Database(server, database)``
        call in ``expressions.tmdl``, and writes the definition back.

        Note: We use the REST API directly rather than sempy (e.g. ``sempy.fabric``)
        because sempy is only available within the Fabric notebook runtime. This module
        is designed to run from local development environments, CI/CD pipelines, and
        other external contexts where the Fabric runtime is not available.
        """
        model_id = self.get_item_id(semantic_model_name, item_type="SemanticModel")
        base = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/semanticModels/{model_id}"

        # --- Fetch current definition (TMDL format) ---
        logger.info("Fetching semantic model definition for '%s' (%s)", semantic_model_name, model_id)
        get_response = self._client.post(f"{base}/getDefinition?format=TMDL")
        definition_response = self._client.handle_lro_response(get_response)
        parts = definition_response.json()["definition"]["parts"]

        # --- Find and update expressions.tmdl ---
        updated_parts = []
        found = False
        for part in parts:
            if part["path"].endswith("expressions.tmdl"):
                content = base64.b64decode(part["payload"]).decode("utf-8")
                original = content

                # Replace Sql.Database("old_server", "old_db") with new values
                content = re.sub(
                    r'Sql\.Database\(\s*"[^"]+"\s*,\s*"[^"]+"\s*\)',
                    f'Sql.Database("{sql_endpoint_server}", "{sql_endpoint_database_id}")',
                    content,
                )

                if content == original:
                    logger.warning("No Sql.Database() call found in expressions.tmdl — skipping update")
                    return

                logger.info("Updated Sql.Database() → server=%s, database=%s",
                            sql_endpoint_server, sql_endpoint_database_id)

                updated_parts.append({
                    "path": part["path"],
                    "payload": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                    "payloadType": "InlineBase64",
                })
                found = True
            else:
                updated_parts.append(part)

        if not found:
            logger.warning("expressions.tmdl not found in semantic model definition — skipping update")
            return

        # --- Write updated definition ---
        logger.info("Updating semantic model definition for '%s'", semantic_model_name)
        update_response = self._client.post(
            f"{base}/updateDefinition",
            json_body={"definition": {"parts": updated_parts}},
        )
        self._client.handle_lro_response(update_response)
        logger.info("Semantic model '%s' updated successfully", semantic_model_name)
