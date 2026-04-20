from urllib.parse import quote

class OneLakeTools:
    
    # Helper function to create base ABFSS path based on workspace and lakehouse name
    @staticmethod
    def construct_base_abfss_path(workspace_name: str, lakehouse_name: str) -> str:
        """Construct the base ABFSS path for a given workspace and lakehouse."""
        # URL encode the names to handle special characters
        workspace_name = quote(workspace_name, safe='')
        lakehouse_name = quote(lakehouse_name, safe='')
        return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"

    # Helper function to create storage options that enable data tools to authenticate and interact with onelake storage
    @staticmethod
    def create_storage_options(token) -> dict:
        return {
            "bearer_token": token,
            "use_fabric_endpoint": "true"
        }