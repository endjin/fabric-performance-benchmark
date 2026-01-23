# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************

## %%configure -f
# {"vCores": 8}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Pandas Benchmark
# 
# This notebook runs a representative end to end use case over data sourced from the [UK Land Registry House Price Data open data repository](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads).
# 
# This data is made available for us under an [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
# 
# We will run two processes:
# 
# 1. Load raw data, clean it up, add new features and finally write it as a mini dimensional model (prices, locations, dates) to the lakehouse.
# 1. Query two of the tables in the dimensional model, join them and summarise the data.

# MARKDOWN ********************

# ## Set up

# CELL ********************

# Common imports
import time
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
import psutil
import notebookutils
import polars as pl

# Imports unique to Pandas version of notebook
import pandas as pd
from deltalake import write_deltalake, DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger = logging.getLogger(name="pandas_benchmark_notebook")
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Pre-requisities are to create a Fabric Workspace with a lakehouse, putting names here:
WORKSPACE_NAME = "fabric_performance_benchmark_workspace"
LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"

# Path where raw data will be downloaded to
RAW_DATA_RELATIVE_PATH = "land_registry"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Common code used to set up all notebooks

# Helper function to create base ABFSS path based on workspace and lakehouse name
def construct_base_abfss_path(workspace_name: str, lakehouse_name: str) -> str:
    """Construct the base ABFSS path for a given workspace and lakehouse."""
    # Because it is a URL, replace spaces with %20
    workspace_name = workspace_name.replace(" ", "%20")
    lakehouse_name = lakehouse_name.replace(" ", "%20")
    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"

# Contruct base path
source_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Files/{RAW_DATA_RELATIVE_PATH}"

# Helper function to create storage options that enable data tools to authenticate and interact with onelake storage
def create_storage_options() -> dict:
    return {
        "bearer_token": notebookutils.credentials.getToken('storage'),
        "use_fabric_endpoint": "true"
    }

# Data class to store benchmark metrics at key points during notebook process 
@dataclass
class Benchmark:
    workload_name: str
    run_timestamp: str
    stage_name: str
    stage_time: float
    cpu: float
    memory: float

# Benchmark Manager class will help capture benchmarks consistently, then write them out to lakehouse at end of notebook
class BenchmarkManager:

    benchmarks = []

    def __init__(self, workload_name: str, run_timestamp: str, export_abfss_path:str, storage_options:dict):
        self.workload_name=workload_name
        self.run_timestamp=run_timestamp
        self.export_abfss_path=export_abfss_path
        self.storage_options=storage_options
    
    def capture_benchmark(self, stage_name):
        self.benchmarks.append(
            Benchmark(
                workload_name=self.workload_name,
                run_timestamp=self.run_timestamp,
                stage_name=stage_name,
                stage_time=time.perf_counter(),
                cpu=psutil.cpu_percent(interval=None),
                memory=psutil.virtual_memory().percent,
            )
        )
    
    def export_results(self):
        records_to_export = pl.DataFrame([asdict(benchmark) for benchmark in self.benchmarks])
        records_to_export = (
            records_to_export
            .sort("stage_time", descending=False)
            .with_row_index("order", offset=1)
            .with_columns((pl.col("stage_time") - pl.col("stage_time").shift(1)).alias("stage_time_delta"))
        )
        records_to_export.write_delta(self.export_abfss_path, mode="append", storage_options=self.storage_options)
        return records_to_export


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Contruct source path for raw data
source_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Files/{RAW_DATA_RELATIVE_PATH}/*.csv"

# Construct base path for lakehouse schema
schema_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/pandas_benchmark_{run_timestamp}"

# Now construct paths to tables in that schema
target_path_prices = f"{schema_path}/prices"
target_path_locations = f"{schema_path}/locations"
target_path_dates = f"{schema_path}/dates"

# Create storage options
storage_options = create_storage_options()

# Set up benchmark manager
benchmark_manager = BenchmarkManager(
    workload_name="Pandas Benchmark",
    run_timestamp=run_timestamp,
    export_abfss_path=f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmarks",
    storage_options=storage_options
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("start")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Ingest Raw Data

# CELL ********************

source_files = [file.path for file in notebookutils.fs.ls(source_path)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Unfortunately we hit out of memory exception when we load all 30 years, so to allow the notebook to complete, we'll process 10
source_files = source_files[0:10]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Reading price paid data from location {source_path}...")

# Define column names and types
column_names = [
    "transaction_unique_identifier",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",
    "record_status"
]

column_dtypes = {
    "transaction_unique_identifier": str,
    "price": float,
    "postcode": str,
    "property_type": str,
    "old_new": str,
    "duration": str,
    "paon": str,
    "saon": str,
    "street": str,
    "locality": str,
    "town_city": str,
    "district": str,
    "county": str,
    "ppd_category_type": str,
    "record_status": str
}

logger.info(f"Ingesting {len(source_files)} source files:")

# Read and concatenate all CSV files
price_paid_data = pd.DataFrame()

for file_number, source_file in enumerate(source_files):
    logger.info(f"Loading file {file_number + 1} of {len(source_files)}")
    df = pd.read_csv(
        source_file,
        header=None,
        na_values=[""],
        names=column_names,
        dtype=column_dtypes,
        storage_options=storage_options,
    )
    price_paid_data = pd.concat([price_paid_data, df], ignore_index=True)

logger.info(f"Loaded all source files into single dataframe.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("ingest")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Data Transformation
# 
# Now we have the DataFrame loaded, we can start to build up the transformations we want to apply:

# CELL ********************

# Convert the property_type column from single letter codes to full descriptions
property_type_mapping = {
    "D": "Detached",
    "S": "Semi-Detached",
    "T": "Terraced",
    "F": "Flat/Maisonette",
    "O": "Other"
}
price_paid_data["property_type"] = price_paid_data["property_type"].map(property_type_mapping).fillna(price_paid_data["property_type"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Do the same for old_new
old_new_mapping = {
    "Y": "New",
    "N": "Old"
}
price_paid_data["old_new"] = price_paid_data["old_new"].map(old_new_mapping).fillna(price_paid_data["old_new"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Use regex to extract the postcode area (the first one or two letters)
price_paid_data["postcode_area"] = price_paid_data["postcode"].str.extract(r"^([A-Z]{1,2})", expand=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Convert date_of_transfer from string to datetime
price_paid_data["date_of_transfer"] = pd.to_datetime(price_paid_data["date_of_transfer"], format='%Y-%m-%d %H:%M', errors='coerce', utc=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Conver date_of_transfer to date
price_paid_data["date_of_transfer"] = price_paid_data["date_of_transfer"].dt.date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("transform")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create fact table
# 
# Select the core columns we want to use in the core fact table.

# CELL ********************

# Select relevant columns for downstream analysis
prices = price_paid_data[[
    "price",
    "date_of_transfer",
    "postcode_area",
    "town_city",
    "property_type",
    "old_new",
]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("create_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create date dimension
# 
# Use min and max dates to build date dimension table.

# CELL ********************

min_date = price_paid_data["date_of_transfer"].min()
max_date = price_paid_data["date_of_transfer"].max()
min_date, max_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

date_range = pd.date_range(start=min_date, end=max_date, freq="D")
dates = pd.DataFrame({"date": date_range})
dates["year"] = dates["date"].dt.year
dates["month"] = dates["date"].dt.month
dates["month_name"] = dates["date"].dt.strftime("%B")
dates["day"] = dates["date"].dt.day
dates["weekday"] = dates["date"].dt.weekday
dates["weekday_name"] = dates["date"].dt.strftime("%A")
dates["day_of_year"] = dates["date"].dt.dayofyear
dates["date"] = dates["date"].dt.date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("create_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create location dimension
# 
# Assumption is there is a hierarchy in descreasing order of granularity:
# 
# - County
# - District
# - Town or City

# CELL ********************

locations = price_paid_data[[
    "county",
    "district",
    "town_city",
]].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("create_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Writing to Delta Tables
# 
# It is common practice to write out a Pandas DataFrame to a Delta table in the Tables area of your Lakehouse.
# 
# There are various write modes which are available:
# 
# Overwrite entire table:
# 
# ```python
# write_deltalake(path, df, mode="overwrite")
# ```
# 
# Append to existing table:
# 
# ```python
# write_deltalake(path, df, mode="append")
# ```
# 
# Merge (upsert) - use DeltaTable API:
# 
# ```python
# dt = DeltaTable(path)
# (
#     dt.merge(
#         source=df,
#         predicate="source.id = target.id",
#         source_alias="source",
#         target_alias="target"
#     )
#     .when_matched_update_all()
#     .when_not_matched_insert_all()
#     .execute()
# )
# ```

# MARKDOWN ********************

# ### Handling Timestamps
# 
# A common gotcha when writing Delta tables from Pandas is timezone handling. Fabric's SQL endpoint expects timestamps with timezone information.
# 
# We can address this by adding timezone information, for example:
# 
# ```python
# df["datetime_of_order"] = df["datetime_of_order"].dt.tz_localize("UTC")
# ```

# MARKDOWN ********************

# ### Write Prices

# CELL ********************

target_path_prices

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Writing prices data to Parquet: {target_path_prices}")
write_deltalake(target_path_prices, prices, mode='overwrite', schema_mode='merge', engine='rust', storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("write_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Write Locations

# CELL ********************

logger.info(f"Writing locations data to Parquet: {target_path_locations}")
write_deltalake(
    target_path_locations,
    locations,
    mode='overwrite',
    schema_mode='merge',
    engine='rust',
    storage_options=storage_options
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("write_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Write Dates

# CELL ********************

logger.info(f"Writing dates data to Parquet: {target_path_dates}")
write_deltalake(target_path_dates, dates, mode='overwrite', schema_mode='merge', engine='rust', storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("write_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Reading from DeltaLake and generate summary
# 
# Let's illustrate this by generating some analytics in this notebook using the data we have just written to the lakehouse in Delta format.

# MARKDOWN ********************

# ### Read Prices

# CELL ********************

# Load prices from Parquet and filter them to exclude "Other" property types
logger.info(f"Reading prices data back from Parquet: {target_path_prices}")
prices = DeltaTable(target_path_prices, storage_options=storage_options).to_pyarrow_dataset().to_table().to_pandas()
prices = prices[prices["property_type"] != "Other"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("read_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

prices.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

prices.loc[0:3, "date_of_transfer"][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Read Dates

# CELL ********************

# Load the date dimension, add a new month_tag column in the form YYYY_MM
logger.info(f"Reading dates data back from Parquet: {target_path_dates}")
dates = DeltaTable(target_path_dates, storage_options=storage_options).to_pyarrow_dataset().to_table().to_pandas()
dates["date"] = pd.to_datetime(dates["date"])
dates["month_tag"] = dates["date"].dt.strftime("%Y_%m")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

dates["date"] = dates["date"].dt.date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("read_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Join and Summarise

# CELL ********************

# Now join the two tables to get month_tag into the prices table
# prices["date_of_transfer"] = pd.to_datetime(prices["date_of_transfer"])
prices = prices.merge(
    dates[["date", "month_tag"]],
    left_on="date_of_transfer",
    right_on="date",
    how="left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

prices.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Finally summarise the data up to monthly level by property type
monthly_summary = (
    prices
    .groupby(["month_tag", "property_type"])
    .agg(
        number_of_transactions=("price", "count"),
        median_price=("price", "median"),
        min_price=("price", "min"),
        max_price=("price", "max"),
    )
    .reset_index()
    .sort_values(["month_tag", "property_type"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

monthly_summary.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("join_and_summarise")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_results = benchmark_manager.export_results()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

elapsed = benchmark_results["stage_time"].max() - benchmark_results["stage_time"].min()
logger.info(f"Notebook completed in {elapsed:.2f} seconds.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
