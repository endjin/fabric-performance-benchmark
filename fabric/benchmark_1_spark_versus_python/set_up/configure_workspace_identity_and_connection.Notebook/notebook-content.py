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

# ## Setup: Get workspace ID and auth token

# CELL ********************

# Get the GUID of the current workspace
current_workspace_id = notebookutils.runtime.context.get('currentWorkspaceId')
print(f"Workspace ID: {current_workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Get credentials to call the Fabric REST API
bearer_token = notebookutils.credentials.getToken("pbi")
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 1: Provision workspace identity
# 
# Creates a workspace identity (managed service principal) for the current workspace.
# If the identity already exists, this step is skipped gracefully.
# 
# API reference: [Provision Identity](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/provision-identity)

# CELL ********************

provision_url = f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/provisionIdentity"
logger.debug(f"Calling provisionIdentity: POST {provision_url}")

provision_response = requests.post(provision_url, headers=headers)
logger.debug(f"provisionIdentity HTTP status: {provision_response.status_code}")
logger.debug(f"provisionIdentity response headers: {dict(provision_response.headers)}")

workspace_identity = None

if provision_response.status_code == 200:
    # Identity provisioned immediately (or already existed)
    workspace_identity = provision_response.json()
    print(f"✓ Workspace identity provisioned successfully")
    print(f"  Application ID:         {workspace_identity.get('applicationId')}")
    print(f"  Service Principal ID:   {workspace_identity.get('servicePrincipalId')}")

elif provision_response.status_code == 202:
    # Long-running operation — poll until complete
    location = provision_response.headers.get("Location")
    logger.debug(f"Async provisioning started. Polling location: {location}")
    
    operation_response = poll_long_running_operation(location, headers)
    
    # Fetch the result from the operation's resource location
    result_location = operation_response.headers.get("Location")
    if result_location:
        logger.debug(f"Fetching result from location: {result_location}")
        result_response = requests.get(result_location, headers=headers)
        workspace_identity = result_response.json()
    else:
        workspace_identity = operation_response.json()

    print(f"✓ Workspace identity provisioned successfully")
    print(f"  Application ID:         {workspace_identity.get('applicationId')}")
    print(f"  Service Principal ID:   {workspace_identity.get('servicePrincipalId')}")

else:
    # Check if the error indicates the identity already exists
    error_body = provision_response.json() if provision_response.content else {}
    error_code = error_body.get("errorCode", "")
    
    if "already" in error_code.lower() or "already" in error_body.get("message", "").lower() or "exist" in error_body.get("message", "").lower():
        print(f"ℹ Workspace identity already exists (HTTP {provision_response.status_code})")
        logger.debug(f"Existing identity response: {json.dumps(error_body, indent=2)}")
    else:
        raise Exception(
            f"Failed to provision workspace identity (HTTP {provision_response.status_code}): "
            f"{json.dumps(error_body, indent=2)}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 2: Grant workspace identity contributor access
# 
# Assigns the workspace identity (service principal) the **Contributor** role on the workspace,
# if it doesn't already have it.
# 
# API references:
# - [List Workspace Role Assignments](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspace-role-assignments)
# - [Add Workspace Role Assignment](https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/add-workspace-role-assignment)

# CELL ********************

# Get the service principal ID — either from provisioning above, or retrieve current role assignments
# to find the workspace identity's service principal ID
roles_url = f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/roleAssignments"
roles_response = requests.get(roles_url, headers=headers)
roles_response.raise_for_status()
role_assignments = roles_response.json().get("value", [])

logger.debug(f"Current role assignments: {json.dumps(role_assignments, indent=2)}")

# Find the workspace identity service principal ID if we don't have it from provisioning
if workspace_identity is None:
    # The workspace identity's display name matches the workspace name
    # Look for a ServicePrincipal in the role assignments that might be the workspace identity
    # We'll use the applicationId from the workspace settings instead
    logger.debug("workspace_identity is None — identity was already provisioned in a previous run")

service_principal_id = workspace_identity.get("servicePrincipalId") if workspace_identity else None
print(f"Service Principal ID for role assignment: {service_principal_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if service_principal_id is None:
    print("⚠ Could not determine workspace identity service principal ID.")
    print("  The workspace identity may already have been provisioned in an earlier session.")
    print("  Please check workspace settings to confirm the identity and its role assignment.")
else:
    # Check if the service principal already has a role assignment
    existing_assignment = None
    for assignment in role_assignments:
        principal = assignment.get("principal", {})
        if principal.get("id") == service_principal_id and principal.get("type") == "ServicePrincipal":
            existing_assignment = assignment
            break

    if existing_assignment:
        existing_role = existing_assignment.get("role")
        print(f"ℹ Workspace identity already has role: {existing_role}")
        if existing_role != "Contributor":
            print(f"  Note: Current role is '{existing_role}', not 'Contributor'. No change made.")
    else:
        # Add Contributor role assignment
        add_role_url = f"https://api.fabric.microsoft.com/v1/workspaces/{current_workspace_id}/roleAssignments"
        role_payload = {
            "principal": {
                "id": service_principal_id,
                "type": "ServicePrincipal"
            },
            "role": "Contributor"
        }
        
        logger.debug(f"Adding role assignment: POST {add_role_url}")
        logger.debug(f"Role payload: {json.dumps(role_payload, indent=2)}")
        
        add_role_response = requests.post(add_role_url, headers=headers, json=role_payload)
        
        if add_role_response.status_code == 201:
            print(f"✓ Workspace identity granted Contributor access")
            logger.debug(f"Role assignment response: {add_role_response.json()}")
        else:
            error_body = add_role_response.json() if add_role_response.content else {}
            raise Exception(
                f"Failed to add role assignment (HTTP {add_role_response.status_code}): "
                f"{json.dumps(error_body, indent=2)}"
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 3: Discover the connection type for Fabric Data Pipelines
# 
# Queries the supported connection types to find the exact type string and required parameters
# for creating a "Fabric Data Pipelines" connection with workspace identity authentication.
# 
# API reference: [List Supported Connection Types](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/list-supported-connection-types)

# CELL ********************

supported_types_url = "https://api.fabric.microsoft.com/v1/connections/supportedConnectionTypes"
logger.debug(f"Fetching supported connection types: GET {supported_types_url}")

all_connection_types = []
next_url = supported_types_url

while next_url:
    types_response = requests.get(next_url, headers=headers)
    types_response.raise_for_status()
    body = types_response.json()
    all_connection_types.extend(body.get("value", []))
    
    # Handle pagination
    continuation = body.get("continuationToken")
    if continuation:
        next_url = f"{supported_types_url}?continuationToken={continuation}"
    else:
        next_url = None

print(f"Found {len(all_connection_types)} supported connection types")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Search for connection types that support WorkspaceIdentity authentication
# and match "Fabric Data Pipelines" (or similar naming)
pipeline_candidates = []

for ct in all_connection_types:
    type_name = ct.get("type", "")
    supported_creds = ct.get("supportedCredentialTypes", [])
    
    # Check if WorkspaceIdentity is supported
    if "WorkspaceIdentity" in supported_creds:
        # Look for pipeline-related types
        if any(keyword in type_name.lower() for keyword in ["pipeline", "datafactory", "fabricdatapipeline", "datapipeline"]):
            pipeline_candidates.append(ct)

if pipeline_candidates:
    print("Pipeline-related connection types supporting WorkspaceIdentity:")
    for candidate in pipeline_candidates:
        print(f"\n  Type: {candidate['type']}")
        print(f"  Supported credentials: {candidate.get('supportedCredentialTypes', [])}")
        for method in candidate.get("creationMethods", []):
            print(f"  Creation method: {method['name']}")
            print(f"  Parameters: {json.dumps(method.get('parameters', []), indent=4)}")
else:
    # Wider search — show all types supporting WorkspaceIdentity for manual inspection
    print("No pipeline-specific types found. Types supporting WorkspaceIdentity:")
    for ct in all_connection_types:
        if "WorkspaceIdentity" in ct.get("supportedCredentialTypes", []):
            print(f"  - {ct['type']} (credentials: {ct.get('supportedCredentialTypes', [])})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Set the connection type details based on discovery above.
# Update these values if the discovered type name differs from the default.
#
# Common Fabric Data Pipelines connection type values:
#   type = "FabricDataPipelines" or "DataPipelines"
#   creationMethod = the name from the creation method in the discovery output
#   parameters = as listed in the discovery output (often none required)
#
# After running the discovery cell above, update the values below if needed:

connection_type = "FabricDataPipelines"  # Update based on discovery output
creation_method = "FabricDataPipelines"  # Update based on discovery output
connection_parameters = []               # Update based on discovery output

print(f"Using connection type:    {connection_type}")
print(f"Using creation method:    {creation_method}")
print(f"Using parameters:         {connection_parameters}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Step 4: Create the shared cloud connection
# 
# Creates a shared cloud connection for Fabric Data Pipelines using workspace identity authentication.
# If a connection with the same name already exists, the existing connection GUID is retrieved.
# 
# API reference: [Create Connection](https://learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection)

# CELL ********************

connection_display_name = "Fabric Data Pipelines (Workspace Identity)"

connection_payload = {
    "connectivityType": "ShareableCloud",
    "displayName": connection_display_name,
    "connectionDetails": {
        "type": connection_type,
        "creationMethod": creation_method,
        "parameters": connection_parameters
    },
    "privacyLevel": "Organizational",
    "credentialDetails": {
        "singleSignOnType": "None",
        "connectionEncryption": "NotEncrypted",
        "skipTestConnection": False,
        "credentials": {
            "credentialType": "WorkspaceIdentity"
        }
    }
}

create_connection_url = "https://api.fabric.microsoft.com/v1/connections"
logger.debug(f"Creating connection: POST {create_connection_url}")
logger.debug(f"Connection payload: {json.dumps(connection_payload, indent=2)}")

create_response = requests.post(create_connection_url, headers=headers, json=connection_payload)
logger.debug(f"Create connection HTTP status: {create_response.status_code}")
logger.debug(f"Create connection response headers: {dict(create_response.headers)}")

connection_id = None

if create_response.status_code == 201:
    connection_data = create_response.json()
    connection_id = connection_data["id"]
    print(f"✓ Connection created successfully")
    print(f"  Connection ID:   {connection_id}")
    print(f"  Display name:    {connection_data.get('displayName')}")
    print(f"  Type:            {connection_data.get('connectivityType')}")
    print(f"  Privacy level:   {connection_data.get('privacyLevel')}")
    logger.debug(f"Full connection response: {json.dumps(connection_data, indent=2)}")
else:
    error_body = create_response.json() if create_response.content else {}
    error_code = error_body.get("errorCode", "")
    error_message = error_body.get("message", "")
    
    if "duplicate" in error_code.lower() or "duplicate" in error_message.lower() or "already" in error_message.lower():
        print(f"ℹ Connection '{connection_display_name}' already exists. Retrieving existing connection...")
    else:
        raise Exception(
            f"Failed to create connection (HTTP {create_response.status_code}): "
            f"{json.dumps(error_body, indent=2)}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# If the connection already existed, find it by name
if connection_id is None:
    list_connections_url = "https://api.fabric.microsoft.com/v1/connections"
    logger.debug(f"Listing connections: GET {list_connections_url}")
    
    list_response = requests.get(list_connections_url, headers=headers)
    list_response.raise_for_status()
    
    connections = list_response.json().get("value", [])
    for conn in connections:
        if conn.get("displayName") == connection_display_name:
            connection_id = conn["id"]
            print(f"✓ Found existing connection")
            print(f"  Connection ID:   {connection_id}")
            print(f"  Display name:    {conn.get('displayName')}")
            print(f"  Type:            {conn.get('connectivityType')}")
            print(f"  Privacy level:   {conn.get('privacyLevel')}")
            break
    
    if connection_id is None:
        raise Exception(
            f"Connection '{connection_display_name}' was reported as duplicate but could not be found. "
            f"Check 'Manage connections and gateways' in the Fabric portal."
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Result
# 
# The connection GUID below can be used downstream — for example, to update the variable library
# in `configure_notebook_guids.Notebook`.

# CELL ********************

print(f"\n{'='*60}")
print(f"  Connection GUID: {connection_id}")
print(f"{'='*60}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
