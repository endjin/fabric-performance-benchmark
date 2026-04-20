import json
import logging

from ._client import FABRIC_API_BASE, FabricRestClient

logger = logging.getLogger(__name__)


class FabricConnections:
    """Create and discover Fabric shared cloud connections."""

    def __init__(self, client: FabricRestClient):
        self._client = client

    def list_supported_types(self) -> list[dict]:
        """Return all supported connection types (handles pagination)."""
        base_url = f"{FABRIC_API_BASE}/v1/connections/supportedConnectionTypes"
        logger.info("Fetching supported connection types")

        all_types: list[dict] = []
        url: str | None = base_url

        while url:
            response = self._client.get(url)
            response.raise_for_status()
            body = response.json()
            page = body.get("value", [])
            all_types.extend(page)

            continuation = body.get("continuationToken")
            url = f"{base_url}?continuationToken={continuation}" if continuation else None

        logger.info("Found %d supported connection types", len(all_types))
        return all_types

    def find_connection_type(
        self,
        keywords: list[str],
        credential_type: str = "WorkspaceIdentity",
    ) -> list[dict]:
        """Search supported types for those matching *keywords* and supporting
        *credential_type*.

        Returns a list of matching type dicts (may be empty).
        """
        all_types = self.list_supported_types()
        matches: list[dict] = []

        for ct in all_types:
            type_name = ct.get("type", "")
            supported_creds = ct.get("supportedCredentialTypes", [])

            if credential_type not in supported_creds:
                continue

            if any(kw.lower() in type_name.lower() for kw in keywords):
                matches.append(ct)
                logger.info(
                    "Matched connection type '%s' (credentials: %s)",
                    type_name,
                    supported_creds,
                )
                for method in ct.get("creationMethods", []):
                    logger.info(
                        "  Creation method '%s', parameters: %s",
                        method["name"],
                        json.dumps(method.get("parameters", []), indent=2),
                    )

        if not matches:
            logger.warning(
                "No connection types matched keywords=%s with credential_type='%s'. "
                "Listing all types that support '%s':",
                keywords,
                credential_type,
                credential_type,
            )
            for ct in all_types:
                if credential_type in ct.get("supportedCredentialTypes", []):
                    logger.info("  %s", ct["type"])

        return matches

    def create_cloud_connection(
        self,
        display_name: str,
        connection_type: str,
        creation_method: str,
        parameters: list[dict] | None = None,
        credential_type: str = "WorkspaceIdentity",
        privacy_level: str = "Organizational",
    ) -> str:
        """Create a shareable cloud connection and return its GUID.

        If a connection with *display_name* already exists, its GUID is
        returned instead.
        """
        url = f"{FABRIC_API_BASE}/v1/connections"
        payload = {
            "connectivityType": "ShareableCloud",
            "displayName": display_name,
            "connectionDetails": {
                "type": connection_type,
                "creationMethod": creation_method,
                "parameters": parameters or [],
            },
            "privacyLevel": privacy_level,
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection": False,
                "credentials": {
                    "credentialType": credential_type,
                },
            },
        }

        logger.info(
            "Creating cloud connection '%s' (type=%s, credential=%s)",
            display_name,
            connection_type,
            credential_type,
        )
        logger.debug("Connection payload: %s", json.dumps(payload, indent=2))

        response = self._client.post(url, json_body=payload)

        if response.status_code == 201:
            connection_data = response.json()
            connection_id = connection_data["id"]
            logger.info("Connection created — id=%s", connection_id)
            return connection_id

        # Handle duplicate
        error_body = response.json() if response.content else {}
        error_code = error_body.get("errorCode", "")
        message = error_body.get("message", "")

        if "duplicate" in error_code.lower() or "duplicate" in message.lower() or "already" in message.lower():
            logger.info("Connection '%s' already exists — looking up existing GUID", display_name)
            existing_id = self.find_connection_by_name(display_name)
            if existing_id:
                return existing_id
            raise RuntimeError(
                f"Connection '{display_name}' reported as duplicate but could not be found via API"
            )

        raise RuntimeError(
            f"Failed to create connection (HTTP {response.status_code}): "
            f"{json.dumps(error_body, indent=2)}"
        )

    def find_connection_by_name(self, display_name: str) -> str | None:
        """Search existing connections for one matching *display_name*."""
        url = f"{FABRIC_API_BASE}/v1/connections"
        logger.debug("Listing connections to find '%s'", display_name)

        response = self._client.get(url)
        response.raise_for_status()

        for conn in response.json().get("value", []):
            if conn.get("displayName") == display_name:
                conn_id = conn["id"]
                logger.info("Found existing connection '%s' → %s", display_name, conn_id)
                return conn_id

        logger.warning("Connection '%s' not found", display_name)
        return None
