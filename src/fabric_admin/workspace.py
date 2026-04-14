import json
import logging

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
