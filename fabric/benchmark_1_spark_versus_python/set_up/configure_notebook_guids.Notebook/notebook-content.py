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

# Looking at the API for Fabric, here are a couple of endpoints we can use:
# POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/variableLibraries/{variableLibraryId}/getDefinition
# POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/variableLibraries/{variableLibraryId}/updateDefinition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Haven't figured out a way of programmatically getting vairable library based on name (similar to notebookutils.notebook.get() above).
variable_library_id = "d088e269-3485-41af-8bfd-6e9e99c6f79a"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

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

response = requests.post(
    url=f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/variableLibraries/{variable_library_id}/getDefinition",
    headers=headers
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

response.headers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

location = response.headers.get("Location")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

location_response = requests.get(location)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

location_response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
