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

notebook_name = "pyspark_benchmark"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

current_workspace_id = notebookutils.runtime.context.get('currentWorkspaceId')
current_workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebookutils.notebook.get(name=notebook_name, workspaceId=current_workspace_id).id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/variableLibraries/{variableLibraryId}/getDefinition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

variable_library_id = "d088e269-3485-41af-8bfd-6e9e99c6f79a"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

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

response.headers.get("Location")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

location_response = requests.get(response.headers.get("Location"))

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
