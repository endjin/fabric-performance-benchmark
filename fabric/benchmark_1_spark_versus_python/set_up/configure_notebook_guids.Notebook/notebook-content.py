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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Helper: Poll long-running operation
# 
# Fabric's `getDefinition` and `updateDefinition` APIs are asynchronous long-running operations. They return a 202 with a `Location` header URL that must be polled (with auth headers) until the operation completes.

# CELL ********************

def poll_long_running_operation(location_url, headers, max_retries=10, retry_interval=2):
    """Poll a Fabric long-running operation URL until it completes."""
    for attempt in range(max_retries):
        response = requests.get(location_url, headers=headers)
        
        if response.status_code == 200:
            return response
        elif response.status_code == 202:
            # Operation still in progress — check for Retry-After header
            retry_after = int(response.headers.get("Retry-After", retry_interval))
            print(f"Operation in progress, retrying in {retry_after}s (attempt {attempt + 1}/{max_retries})...")
            time.sleep(retry_after)
        else:
            raise Exception(f"Unexpected status {response.status_code}: {response.text}")
    
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

# ## Look up Variable Library by name
# 
# Use the Items API to find the Variable Library ID by name, then retrieve its definition via the long-running `getDefinition` API.

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

# Look up the Variable Library ID by name using the Items API
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

print(f"Variable Library ID: {variable_library_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Get the current Variable Library definition
response = requests.post(
    url=f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/variableLibraries/{variable_library_id}/getDefinition",
    headers=headers
)

if response.status_code == 200:
    # Definition returned directly
    definition_response = response
elif response.status_code == 202:
    # Long-running operation — poll the Location URL
    location = response.headers.get("Location")
    print(f"Async operation, polling: {location}")
    definition_response = poll_long_running_operation(location, headers)
else:
    raise Exception(f"Unexpected status {response.status_code}: {response.text}")

definition_response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Decode the variables.json part from the definition response
definition_parts = definition_response.json()["definition"]["parts"]
variables_part = next(p for p in definition_parts if p["path"] == "variables.json")
variables_payload = json.loads(base64.b64decode(variables_part["payload"]).decode("utf-8"))

print(json.dumps(variables_payload, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
