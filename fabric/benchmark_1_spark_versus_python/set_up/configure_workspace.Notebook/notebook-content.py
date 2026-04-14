# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Configure Workspace Identity and Connection
# 
# This notebook configures the current Fabric workspace for the benchmarking solution:
# 
# 1. **Provision workspace identity** — creates a managed service principal for the workspace
# 2. **Grant contributor access** — assigns the workspace identity the Contributor role
# 3. **Create shared cloud connection** — creates a "Fabric Data Pipelines (Workspace Identity)" connection
# 4. **Update variable library** — writes the notebook GUID and connection GUID into the variable library
# 
# This notebook runs **inside Fabric** and uses `notebookutils` for authentication and workspace context.

# CELL ********************

import base64
import json
import logging
import time

import notebookutils
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Core REST client
# 
# Adapted from `src/fabric_admin/_client.py` — uses `notebookutils.credentials.getToken` instead
# of `azure.identity.InteractiveBrowserCredential`.

# CELL ********************

class FabricRestClient:
    """Low-level client for the Fabric REST API using notebookutils for auth."""

    def __init__(self, token: str | None = None):
        if token:
            self._token = token
        else:
            logger.info("Acquiring token via notebookutils")
            self._token = notebookutils.credentials.getToken("pbi")
            logger.info("Token acquired successfully")

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def get(self, url: str, **kwargs) -> requests.Response:
        logger.debug("GET %s", url)
        response = requests.get(url, headers=self.headers, **kwargs)
        logger.debug("GET %s → %s", url, response.status_code)
        return response

    def post(self, url: str, json_body: dict | None = None, **kwargs) -> requests.Response:
        logger.debug("POST %s", url)
        response = requests.post(url, headers=self.headers, json=json_body, **kwargs)
        logger.debug("POST %s → %s", url, response.status_code)
        return response

    def poll_long_running_operation(
        self, location_url: str, max_retries: int = 10, retry_interval: int = 2,
    ) -> requests.Response:
        for attempt in range(1, max_retries + 1):
            logger.debug("LRO poll attempt %d/%d: GET %s", attempt, max_retries, location_url)
            response = self.get(location_url)
            body = response.json()
            status = body.get("status")
            logger.debug("LRO status: %s", status)

            if status == "Succeeded":
                return response
            if status in ("Running", "NotStarted", "Undefined"):
                wait = int(response.headers.get("Retry-After", retry_interval))
                logger.debug("LRO in progress — waiting %ds", wait)
                time.sleep(wait)
            elif status == "Failed":
                error = body.get("error", {})
                raise RuntimeError(f"Long-running operation failed: {json.dumps(error, indent=2)}")
            else:
                raise RuntimeError(f"Unexpected LRO status '{status}': {json.dumps(body, indent=2)}")

        raise TimeoutError(f"Long-running operation did not complete after {max_retries} retries")

    def handle_lro_response(self, response: requests.Response) -> requests.Response:
        if response.status_code == 200:
            return response
        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise RuntimeError("202 response missing Location header for LRO polling")
            logger.debug("202 Accepted — polling LRO at %s", location)
            operation_response = self.poll_long_running_operation(location)
            result_location = operation_response.headers.get("Location")
            if result_location:
                logger.debug("Fetching LRO result from %s", result_location)
                return self.get(result_location)
            return operation_response
        response.raise_for_status()
        return response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Workspace operations
# 
# Adapted from `src/fabric_admin/workspace.py` — uses `notebookutils.runtime.context` to get
# the current workspace ID directly rather than resolving by name.

# CELL ********************

class FabricWorkspace:
    """Operations against the current Fabric workspace."""

    def __init__(self, client: FabricRestClient, workspace_id: str):
        self._client = client
        self._workspace_id = workspace_id

    @property
    def workspace_id(self) -> str:
        return self._workspace_id

    def provision_identity(self) -> dict | None:
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/provisionIdentity"
        logger.info("Provisioning workspace identity for workspace %s", self.workspace_id)
        response = self._client.post(url)

        if response.status_code == 200:
            identity = response.json()
            logger.info("Workspace identity provisioned — applicationId=%s, servicePrincipalId=%s",
                        identity.get("applicationId"), identity.get("servicePrincipalId"))
            return identity

        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise RuntimeError("202 response missing Location header for LRO polling")
            logger.info("Workspace identity provisioning accepted — polling LRO")
            operation_response = self._client.poll_long_running_operation(location)
            result_location = operation_response.headers.get("Location")
            if result_location:
                identity = self._client.get(result_location).json()
            else:
                identity = operation_response.json()
            logger.info("Workspace identity provisioned — applicationId=%s, servicePrincipalId=%s",
                        identity.get("applicationId"), identity.get("servicePrincipalId"))
            return identity

        error_body = response.json() if response.content else {}
        error_code = error_body.get("errorCode", "")
        message = error_body.get("message", "")
        if "already" in error_code.lower() or "already" in message.lower() or "exist" in message.lower():
            logger.info("Workspace identity already exists (HTTP %s)", response.status_code)
            return None
        raise RuntimeError(f"Failed to provision workspace identity (HTTP {response.status_code}): "
                           f"{json.dumps(error_body, indent=2)}")

    def list_role_assignments(self) -> list[dict]:
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/roleAssignments"
        logger.debug("Listing role assignments for workspace %s", self.workspace_id)
        response = self._client.get(url)
        response.raise_for_status()
        assignments = response.json().get("value", [])
        logger.debug("Found %d role assignments", len(assignments))
        return assignments

    def add_role_assignment(self, principal_id: str, principal_type: str = "ServicePrincipal",
                            role: str = "Contributor") -> None:
        assignments = self.list_role_assignments()
        for assignment in assignments:
            principal = assignment.get("principal", {})
            if principal.get("id") == principal_id and principal.get("type") == principal_type:
                logger.info("Principal %s already has role '%s' — skipping", principal_id, assignment.get("role"))
                return

        url = f"{FABRIC_API_BASE}/v1/workspaces/{self.workspace_id}/roleAssignments"
        payload = {"principal": {"id": principal_id, "type": principal_type}, "role": role}
        logger.info("Adding %s role for %s (%s)", role, principal_id, principal_type)
        response = self._client.post(url, json_body=payload)
        if response.status_code == 201:
            logger.info("Role assignment created successfully")
        else:
            error_body = response.json() if response.content else {}
            raise RuntimeError(f"Failed to create role assignment (HTTP {response.status_code}): "
                               f"{json.dumps(error_body, indent=2)}")

    def ensure_identity_with_contributor_access(self) -> dict | None:
        identity = self.provision_identity()
        if identity is None:
            logger.warning("Workspace identity already existed — cannot determine service principal ID. "
                           "Verify contributor access manually in workspace settings.")
            return None
        sp_id = identity.get("servicePrincipalId")
        if sp_id:
            self.add_role_assignment(sp_id, principal_type="ServicePrincipal", role="Contributor")
        else:
            logger.warning("No servicePrincipalId in identity response — cannot assign role")
        return identity

    def get_item_id(self, item_name: str, item_type: str) -> str:
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Connection operations
# 
# Adapted from `src/fabric_admin/connections.py`.

# CELL ********************

class FabricConnections:
    """Create and discover Fabric shared cloud connections."""

    def __init__(self, client: FabricRestClient):
        self._client = client

    def list_supported_types(self) -> list[dict]:
        base_url = f"{FABRIC_API_BASE}/v1/connections/supportedConnectionTypes"
        logger.info("Fetching supported connection types")
        all_types: list[dict] = []
        url: str | None = base_url
        while url:
            response = self._client.get(url)
            response.raise_for_status()
            body = response.json()
            all_types.extend(body.get("value", []))
            continuation = body.get("continuationToken")
            url = f"{base_url}?continuationToken={continuation}" if continuation else None
        logger.info("Found %d supported connection types", len(all_types))
        return all_types

    def find_connection_type(self, keywords: list[str],
                              credential_type: str = "WorkspaceIdentity") -> list[dict]:
        all_types = self.list_supported_types()
        matches: list[dict] = []
        for ct in all_types:
            type_name = ct.get("type", "")
            supported_creds = ct.get("supportedCredentialTypes", [])
            if credential_type not in supported_creds:
                continue
            if any(kw.lower() in type_name.lower() for kw in keywords):
                matches.append(ct)
                logger.info("Matched connection type '%s' (credentials: %s)", type_name, supported_creds)
                for method in ct.get("creationMethods", []):
                    logger.info("  Creation method '%s', parameters: %s",
                                method["name"], json.dumps(method.get("parameters", []), indent=2))
        if not matches:
            logger.warning("No types matched keywords=%s with credential_type='%s'. "
                           "Listing all types that support '%s':", keywords, credential_type, credential_type)
            for ct in all_types:
                if credential_type in ct.get("supportedCredentialTypes", []):
                    logger.info("  %s", ct["type"])
        return matches

    def create_cloud_connection(self, display_name: str, connection_type: str, creation_method: str,
                                 parameters: list[dict] | None = None,
                                 credential_type: str = "WorkspaceIdentity",
                                 privacy_level: str = "Organizational") -> str:
        url = f"{FABRIC_API_BASE}/v1/connections"
        payload = {
            "connectivityType": "ShareableCloud",
            "displayName": display_name,
            "connectionDetails": {
                "type": connection_type, "creationMethod": creation_method,
                "parameters": parameters or [],
            },
            "privacyLevel": privacy_level,
            "credentialDetails": {
                "singleSignOnType": "None", "connectionEncryption": "NotEncrypted",
                "skipTestConnection": False,
                "credentials": {"credentialType": credential_type},
            },
        }
        logger.info("Creating cloud connection '%s' (type=%s, credential=%s)",
                     display_name, connection_type, credential_type)
        response = self._client.post(url, json_body=payload)
        if response.status_code == 201:
            connection_id = response.json()["id"]
            logger.info("Connection created — id=%s", connection_id)
            return connection_id
        error_body = response.json() if response.content else {}
        error_code = error_body.get("errorCode", "")
        message = error_body.get("message", "")
        if "duplicate" in error_code.lower() or "duplicate" in message.lower() or "already" in message.lower():
            logger.info("Connection '%s' already exists — looking up existing GUID", display_name)
            existing_id = self.find_connection_by_name(display_name)
            if existing_id:
                return existing_id
            raise RuntimeError(f"Connection '{display_name}' reported as duplicate but could not be found")
        raise RuntimeError(f"Failed to create connection (HTTP {response.status_code}): "
                           f"{json.dumps(error_body, indent=2)}")

    def find_connection_by_name(self, display_name: str) -> str | None:
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Variable library operations
# 
# Adapted from `src/fabric_admin/variable_library.py`.

# CELL ********************

class FabricVariableLibrary:
    """Read and update Fabric Variable Library items via the REST API."""

    def __init__(self, client: FabricRestClient, workspace_id: str):
        self._client = client
        self._workspace_id = workspace_id

    def find_library(self, library_name: str) -> str:
        url = f"{FABRIC_API_BASE}/v1/workspaces/{self._workspace_id}/items?type=VariableLibrary"
        logger.info("Looking up variable library '%s'", library_name)
        response = self._client.get(url)
        response.raise_for_status()
        for item in response.json().get("value", []):
            if item["displayName"] == library_name:
                library_id = item["id"]
                logger.info("Found variable library '%s' → %s", library_name, library_id)
                return library_id
        raise ValueError(f"Variable library '{library_name}' not found in workspace {self._workspace_id}")

    def get_variables(self, library_id: str) -> tuple[list[dict], dict]:
        url = (f"{FABRIC_API_BASE}/v1/workspaces/{self._workspace_id}"
               f"/variableLibraries/{library_id}/getDefinition")
        logger.info("Fetching definition for variable library %s", library_id)
        response = self._client.post(url)
        definition_response = self._client.handle_lro_response(response)
        parts = definition_response.json()["definition"]["parts"]
        variables_part = next(p for p in parts if p["path"] == "variables.json")
        variables_payload = json.loads(base64.b64decode(variables_part["payload"]).decode("utf-8"))
        logger.info("Decoded variables.json — %d variables found", len(variables_payload.get("variables", [])))
        return parts, variables_payload

    def update_variable(self, library_id: str, variable_name: str, new_value: str) -> None:
        """Update a single variable (convenience wrapper around update_variables)."""
        self.update_variables(library_id, {variable_name: new_value})

    def update_variables(self, library_id: str, updates: dict[str, str]) -> None:
        """Apply all *updates* ({name: value}) in a single read-update-write cycle."""
        parts, variables_payload = self.get_variables(library_id)

        remaining = dict(updates)
        for var in variables_payload["variables"]:
            if var["name"] in remaining:
                old_value = var["value"]
                var["value"] = remaining.pop(var["name"])
                logger.info("Variable '%s' updated: '%s' → '%s'", var["name"], old_value, var["value"])

        if remaining:
            raise ValueError(f"Variables not found in variable library {library_id}: {', '.join(sorted(remaining))}")

        updated_b64 = base64.b64encode(json.dumps(variables_payload, indent=2).encode("utf-8")).decode("utf-8")
        updated_parts = []
        for part in parts:
            if part["path"] == "variables.json":
                updated_parts.append({"path": "variables.json", "payload": updated_b64, "payloadType": "InlineBase64"})
            else:
                updated_parts.append(part)

        url = (f"{FABRIC_API_BASE}/v1/workspaces/{self._workspace_id}"
               f"/variableLibraries/{library_id}/updateDefinition")
        logger.info("Updating definition for variable library %s", library_id)
        response = self._client.post(url, json_body={"definition": {"parts": updated_parts}})
        self._client.handle_lro_response(response)
        logger.info("Variable library updated successfully — %d variable(s) written", len(updates))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# Workspace details
CURRENT_WORKSPACE_NAME = notebookutils.runtime.context.get('currentWorkspaceName')

# Connection settings
CONNECTION_DISPLAY_NAME = f"Fabric Data Pipelines (Workspace Identity) - {CURRENT_WORKSPACE_NAME}"
CONNECTION_TYPE = "FabricDataPipelines"
CREATION_METHOD = "FabricDataPipelines.Actions"
CONNECTION_PARAMETERS: list[dict] = []

# Variable library settings
VARIABLE_LIBRARY_NAME = "benchmark_1_variables"
NOTEBOOK_NAMES = ["pyspark_benchmark", "polars_benchmark", "duckdb_benchmark", "pandas_benchmark"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Authenticate and resolve workspace

# CELL ********************

client = FabricRestClient()

current_workspace_id = notebookutils.runtime.context.get('currentWorkspaceId')
logger.info("Current workspace ID: %s", current_workspace_id)

workspace = FabricWorkspace(client, current_workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 1 & 2: Provision workspace identity with contributor access
# 
# Creates a workspace identity (managed service principal) and assigns it the Contributor role.
# Both operations are idempotent — safe to re-run.

# CELL ********************

identity = workspace.ensure_identity_with_contributor_access()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 3: Discover connection types and create shared cloud connection

# CELL ********************

connections = FabricConnections(client)

# Discover pipeline-related connection types that support WorkspaceIdentity
candidates = connections.find_connection_type(
    keywords=["pipeline", "datafactory", "fabricdatapipeline", "datapipeline"],
    credential_type="WorkspaceIdentity",
)

for ct in candidates:
    for method in ct.get("creationMethods", []):
        logger.info("Candidate: type=%s, creationMethod=%s, params=%s",
                     ct["type"], method["name"], method.get("parameters", []))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create (or find existing) shared cloud connection
connection_id = connections.create_cloud_connection(
    display_name=CONNECTION_DISPLAY_NAME,
    connection_type=CONNECTION_TYPE,
    creation_method=CREATION_METHOD,
    parameters=CONNECTION_PARAMETERS,
)
logger.info("Connection GUID: %s", connection_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 4: Update variable library
# 
# Builds a dict of all variables to set — workspace name, connection GUID, and
# each notebook GUID — then writes them all in a single API call.

# CELL ********************

# Build the full set of variable updates
variable_updates: dict[str, str] = {
    "workspace_name": CURRENT_WORKSPACE_NAME,
    "execute_pipeline_connection_id": connection_id,
}

for notebook_name in NOTEBOOK_NAMES:
    notebook_id = workspace.get_item_id(notebook_name, item_type="Notebook")
    variable_updates[f"{notebook_name}_notebook_id"] = notebook_id

logger.info("Variables to update: %s", list(variable_updates.keys()))

# Write all variables in a single read-update-write cycle
var_lib = FabricVariableLibrary(client, current_workspace_id)
library_id = var_lib.find_library(VARIABLE_LIBRARY_NAME)
var_lib.update_variables(library_id, variable_updates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
