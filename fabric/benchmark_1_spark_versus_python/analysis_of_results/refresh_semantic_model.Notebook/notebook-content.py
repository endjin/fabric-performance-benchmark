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

# # Refresh Semantic Model
# 
# This notebook refreshes the downstream reporting artifacts after new benchmark data
# has been written to the lakehouse:
# 
# 1. **SQL analytics endpoint metadata** — triggers a metadata sync via the Fabric REST API
#    so that the SQL endpoint reflects the latest Delta table schemas.
# 2. **Semantic model framing** — triggers a Direct Lake refresh (framing) so that the
#    Power BI semantic model picks up the latest data from OneLake.

# CELL ********************

import logging

import notebookutils
import requests
import sempy.fabric as fabric

logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)

FABRIC_API_BASE = "https://api.fabric.microsoft.com"
LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"
SEMANTIC_MODEL_NAME = "benchmark_analytics"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Refresh SQL analytics endpoint metadata
# 
# The SQL analytics endpoint automatically mirrors lakehouse Delta tables, but metadata
# can lag behind after bulk writes.  This REST API call forces an immediate sync.
# 
# See: [Refresh SQL analytics endpoint Metadata REST API](https://learn.microsoft.com/rest/api/fabric/sqlendpoint/items/refresh-sql-endpoint-metadata)

# CELL ********************

workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Look up the lakehouse to get its SQL endpoint ID
items_url = f"{FABRIC_API_BASE}/v1/workspaces/{workspace_id}/items?type=Lakehouse"
response = requests.get(items_url, headers=headers)
response.raise_for_status()

lakehouse_id = None
for item in response.json().get("value", []):
    if item["displayName"] == LAKEHOUSE_NAME:
        lakehouse_id = item["id"]
        break
assert lakehouse_id, f"Lakehouse '{LAKEHOUSE_NAME}' not found in workspace {workspace_id}"

lh_url = f"{FABRIC_API_BASE}/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
lh_response = requests.get(lh_url, headers=headers)
lh_response.raise_for_status()
sql_endpoint_id = lh_response.json()["properties"]["sqlEndpointProperties"]["id"]
logger.info("SQL endpoint ID: %s", sql_endpoint_id)

# Trigger the metadata refresh
refresh_url = (
    f"{FABRIC_API_BASE}/v1/workspaces/{workspace_id}"
    f"/sqlEndpoints/{sql_endpoint_id}/refreshMetadata?preview=true"
)
logger.info("Refreshing SQL endpoint metadata: POST %s", refresh_url)
refresh_response = requests.post(refresh_url, headers=headers)
refresh_response.raise_for_status()
logger.info("SQL endpoint metadata refresh completed (HTTP %s)", refresh_response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Refresh semantic model
# 
# Triggers a Direct Lake refresh (framing) of the semantic model. This updates the
# metadata pointers to the latest Delta table parquet files in OneLake, making
# the new data immediately available to Power BI reports.

# CELL ********************

logger.info("Refreshing semantic model '%s'", SEMANTIC_MODEL_NAME)
fabric.refresh_dataset(dataset=SEMANTIC_MODEL_NAME)
logger.info("Semantic model '%s' refresh completed", SEMANTIC_MODEL_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
