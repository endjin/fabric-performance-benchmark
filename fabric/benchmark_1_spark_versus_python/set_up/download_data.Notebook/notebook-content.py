# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # Download Data
# 
# To support this benchmark, we are going to download some open data to prime the "files" area on OneLake with raw data we can analyse.
# 
# The are sourcing this from the [UK Land Registry House Price Data open data repository](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads).
# 
# Data is available for us under an [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). 

# CELL ********************

import notebookutils
import requests
import fsspec

import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create connection to variable library
variable_library = notebookutils.variableLibrary.getLibrary("benchmark_1_variables")

# Retrieve workspace name, lakehouse name and raw source data folder path from variable library
WORKSPACE_NAME = variable_library.workspace_name
LAKEHOUSE_NAME = variable_library.lakehouse_name
RAW_DATA_RELATIVE_PATH = variable_library.raw_data_relative_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def construct_base_abfss_path(workspace_name: str, lakehouse_name: str) -> str:
    """Construct the base ABFSS path for a given workspace and lakehouse."""
    # Because it is a URL, replace spaces with %20
    workspace_name = workspace_name.replace(" ", "%20")
    lakehouse_name = lakehouse_name.replace(" ", "%20")
    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"

def create_storage_options() -> dict:
    return {
        "bearer_token": notebookutils.credentials.getToken('storage'),
        "use_fabric_endpoint": "true"
    }

source_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Files/{RAW_DATA_RELATIVE_PATH}"

storage_options = create_storage_options()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

class LandRegstryImporter:

    HOUSE_PRICE_BASE_URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"

    # Each file is approximately 100MB in size.  Change the number of years to control the total data size.
    def __init__(self, data_download_path: str, storage_options: dict, number_of_years: int = 5):
        self.data_download_path = data_download_path
        self.storage_options = storage_options
        self.number_of_years = number_of_years

        self.list_of_files = [f"pp-{year}.csv" for year in range(2025, 2025 - number_of_years, -1)]

    def download_land_registry_data(self):

        logger.info("Starting download of Land Registry data...")
        
        logger.info(f"Number of files to download: {len(self.list_of_files)}")

        for file_number, file_name in enumerate(self.list_of_files):
        
            remote_file_url = f"{self.HOUSE_PRICE_BASE_URL}{file_name}"
            path_to_save_file = self.data_download_path + "/" + file_name

            # Download the CSV file with streaming enabled to avoid OOM on limited memory
            with requests.get(remote_file_url, stream=True) as response:
                response.raise_for_status()  # Ensure we notice bad responses

                # fsspec automatically handles the protocol (file:// versus abfss://) based on the source_path
                with fsspec.open(path_to_save_file, mode='wb', **self.storage_options) as f:
                    # Write in 1MB chunks
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        f.write(chunk)

            logger.info(f"Downloaded file {file_number + 1} of {len(self.list_of_files)}: filename {file_name} to: {path_to_save_file}")
        logger.info("Completed downloading Land Registry data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

land_registry_importer = LandRegstryImporter(
    data_download_path=source_path,
    storage_options=storage_options,
    number_of_years=1
)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

land_registry_importer.download_land_registry_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

files = notebookutils.fs.ls(source_path)
total_file_size = sum([file.size for file in files]) / (1024 * 1024 * 1024)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Downloaded {len(files)} files, total size {total_file_size:.2f}GB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
