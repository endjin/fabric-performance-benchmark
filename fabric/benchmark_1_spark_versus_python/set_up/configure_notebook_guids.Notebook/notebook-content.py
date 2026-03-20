# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import notebookutils
import requests
import time
import json
import base64
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def poll_long_running_operation(location_url, headers, max_retries=10, retry_interval=2):
    """Poll a Fabric long-running operation URL until it completes.
    
    Fabric LROs return HTTP 200 with a JSON body containing a 'status' field.
    We must check the status field (not HTTP code) to determine completion.
    """
    for attempt in range(max_retries):
        logger.debug(f"Polling attempt {attempt + 1}/{max_retries}: GET {location_url}")
        response = requests.get(location_url, headers=headers)
        logger.debug(f"Poll response HTTP status: {response.status_code}")
        logger.debug(f"Poll response headers: {dict(response.headers)}")
        
        body = response.json()
        logger.debug(f"Poll response body: {json.dumps(body, indent=2)}")
        
        operation_status = body.get("status")
        logger.debug(f"Operation status: {operation_status}")
        
        if operation_status == "Succeeded":
            logger.debug("Operation succeeded")
            return response
        elif operation_status in ("Running", "NotStarted", "Undefined"):
            retry_after = int(response.headers.get("Retry-After", retry_interval))
            logger.debug(f"Operation in progress, retrying in {retry_after}s...")
            time.sleep(retry_after)
        elif operation_status == "Failed":
            error = body.get("error", {})
            raise Exception(f"Operation failed: {error}")
        else:
            raise Exception(f"Unexpected operation status '{operation_status}': {json.dumps(body, indent=2)}")
    
    raise Exception(f"Operation did not complete after {max_retries} retries")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Get Notebook ID from name
# 
# Based on this code being replicated into a new workspace, we need to determine the new notebook ID based on the name of the notebook.

# CELL ********************

# Create connection to variable library
variable_library = notebookutils.variableLibrary.getLibrary("benchmark_1_variables")

# Retrieve workspace name, lakehouse name and raw source data folder path from variable library
workspace_name = variable_library.workspace_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Notebook which we want to grab GUID for
notebook_name = "pyspark_benchmark"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Get the GUID of the current workspace
current_workspace_id = notebookutils.runtime.context.get('currentWorkspaceId')
current_workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Get the GUID of the notebook:
notebook_id = notebookutils.notebook.get(name=notebook_name, workspaceId=current_workspace_id).id
notebook_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Update variable in Fabric variable library
# 
# Now trying to update a variable in a Fabric Variable Library with the notebook_id.  This ID is referenced by pipelines in the solution which orchestrate running the notebook.

# CELL ********************

# Get credentials to call the API
bearer_token = notebookutils.credentials.getToken("pbi")
headers = {"Authorization": f"Bearer {bearer_token}"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

items_response = requests.get(
    url=f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/items?type=VariableLibrary",
    headers=headers
)
items_response.raise_for_status()

variable_library_id = None
for item in items_response.json().get("value", []):
    if item["displayName"] == "benchmark_1_variables":
        variable_library_id = item["id"]
        break

if not variable_library_id:
    raise Exception("Variable library 'benchmark_1_variables' not found in workspace")

variable_library_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.debug(f"Calling getDefinition for variable library {variable_library_id}")
response = requests.post(
    url=f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/variableLibraries/{variable_library_id}/getDefinition",
    headers=headers
)
logger.debug(f"getDefinition HTTP status: {response.status_code}")
logger.debug(f"getDefinition response headers: {dict(response.headers)}")

if response.status_code == 200:
    # Definition returned directly
    logger.debug("Definition returned synchronously")
    definition_response = response
elif response.status_code == 202:
    # Long-running operation — poll the Location URL
    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    logger.debug(f"Async operation started. Operation ID: {operation_id}")
    logger.debug(f"Location URL: {location}")
    
    # Poll until operation completes
    operation_response = poll_long_running_operation(location, headers)
    operation_body = operation_response.json()
    logger.debug(f"Completed operation body: {json.dumps(operation_body, indent=2)}")
    
    # Fetch the actual result from the operation's resource location
    resource_location = operation_body.get("resourceLocation")
    if resource_location:
        logger.debug(f"Fetching result from resourceLocation: {resource_location}")
        definition_response = requests.get(resource_location, headers=headers)
        logger.debug(f"Result HTTP status: {definition_response.status_code}")
    else:
        logger.warning("No resourceLocation in operation response, using operation response as-is")
        definition_response = operation_response
else:
    raise Exception(f"Unexpected status {response.status_code}: {response.text}")

logger.debug(f"Final response keys: {list(definition_response.json().keys())}")
definition_response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

definition_parts = definition_response.json()["definition"]["parts"]
variables_part = next(p for p in definition_parts if p["path"] == "variables.json")
variables_payload = json.loads(base64.b64decode(variables_part["payload"]).decode("utf-8"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

print(json.dumps(variables_payload, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
