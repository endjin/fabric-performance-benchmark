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

# # Configure Workspace
# 
# This notebook configures the current Fabric workspace for the benchmarking solution:
# 
# 1. **Create a cloud connection** (manual step — must be done before running this notebook)
# 2. **Update variable library** — writes the connection GUID and notebook GUIDs into the variable library
# 
# ## Prerequisites: Create a Cloud Connection
# 
# Before running this notebook, you must manually create a shared cloud connection:
# 
# 1. Go to [Power BI Gateway Management](https://app.powerbi.com/groups/me/gateways)
# 2. Click **+ New** to create a new connection
# 3. Set the connection type to **Fabric Data Pipelines**
# 4. Name it something descriptive (e.g. "Fabric Data Pipelines - Benchmark")
# 5. Complete the creation wizard
# 6. Once created, copy the **Connection ID** (GUID) from the connection details
# 7. Paste the GUID into the `CONNECTION_ID` variable in the Configuration cell below
# 
# This notebook runs **inside Fabric** and uses `notebookutils` for authentication and workspace context.

# MARKDOWN ********************

# ## Configuration
# **Important**: Paste the Connection ID (GUID) from the cloud connection you created
# via [Power BI Gateway Management](https://app.powerbi.com/groups/me/gateways) into the
# `CONNECTION_ID` variable below before running.

# CELL ********************

# Paste the Connection ID (GUID) from the cloud connection you created manually
# via https://app.powerbi.com/groups/me/gateways
CONNECTION_ID = ""  # e.g. "12dfc5c1-8a87-4c8a-84bc-cf1d1984a6e1"
assert CONNECTION_ID, "Please set CONNECTION_ID to the GUID of your cloud connection before running."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import base64
import json
import logging
import time

import notebookutils
import requests

# Fabric notebooks pre-configure root logger handlers, so logging.basicConfig() is a no-op.
# Instead, set the format and level on the existing handlers directly.
# See: https://learn.microsoft.com/fabric/data-engineering/author-execute-notebook#python-logging-in-a-notebook
_log_format = "%(asctime)s %(name)s %(levelname)s %(message)s"
_formatter = logging.Formatter(fmt=_log_format)
for _handler in logging.getLogger().handlers:
    _handler.setFormatter(_formatter)
logging.getLogger().setLevel(logging.INFO)

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

# ## Setup
# 
# Setup the parameters which drive the behaviour of the notebook.

# CELL ********************

# Workspace details
CURRENT_WORKSPACE_NAME = notebookutils.runtime.context.get('currentWorkspaceName')

# Variable library settings
VARIABLE_LIBRARY_NAME = "benchmark_1_variables"
NOTEBOOK_NAMES = [
    "pyspark_benchmark", "polars_benchmark", "duckdb_benchmark", "pandas_benchmark",
    "analysis_of_results", "refresh_semantic_model",
]

# Lakehouse and semantic model names (for repointing the Power BI data source)
LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"
SEMANTIC_MODEL_NAME = "benchmark_analytics"

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

# ## Update variable library
# 
# Writes the connection GUID (from the manually-created cloud connection), workspace name,
# and each benchmark notebook's GUID into the variable library so that pipelines can reference them.

# CELL ********************

# Build the full set of variable updates
variable_updates: dict[str, str] = {
    "workspace_name": CURRENT_WORKSPACE_NAME,
    "workspace_id": current_workspace_id,
    "execute_pipeline_connection_id": CONNECTION_ID,
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

# MARKDOWN ********************

# ## Update semantic model
# 
# Repoints the Power BI semantic model's Direct Lake connection to the lakehouse
# in the current workspace. This uses the `sempy` package (pre-installed in Fabric)
# which handles looking up the SQL endpoint and rewriting the model expressions via TOM.

# CELL ********************

from sempy.fabric.semantic_model import update_direct_lake_model_lakehouse_connection

update_direct_lake_model_lakehouse_connection(
    dataset=SEMANTIC_MODEL_NAME,
    lakehouse=LAKEHOUSE_NAME,
)
logger.info("Semantic model '%s' repointed to lakehouse '%s'", SEMANTIC_MODEL_NAME, LAKEHOUSE_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
