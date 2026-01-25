# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # Polars Benchmark
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
import json
import notebookutils

# Imports unique to Polars version of notebook
import polars as pl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger = logging.getLogger(name="polars_benchmark_notebook")
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
schema_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/polars_benchmark_{run_timestamp}"

# Now construct paths to tables in that schema
target_path_prices = f"{schema_path}/prices"
target_path_locations = f"{schema_path}/locations"
target_path_dates = f"{schema_path}/dates"

# Create storage options
storage_options = create_storage_options()

# Set up benchmark manager
benchmark_manager = BenchmarkManager(
    workload_name="Polars Benchmark",
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

price_paid_data = pl.scan_csv(
    source_path,  # AFBSS path to the CSV files in the Files area.
    has_header=False,
    null_values=[""],
    storage_options=storage_options,  # Provides Polars with the necessary credentials to read from Fabric.
    infer_schema=False,
    schema={
        "transaction_unique_identifier": pl.Utf8,
        "price": pl.Float64,
        "date_of_transfer": pl.Datetime,
        "postcode": pl.Utf8,
        "property_type": pl.Utf8,
        "old_new": pl.Utf8,
        "duration": pl.Utf8,
        "paon": pl.Utf8,
        "saon": pl.Utf8,
        "street": pl.Utf8,
        "locality": pl.Utf8,
        "town_city": pl.Utf8,
        "district": pl.Utf8,
        "county": pl.Utf8,
        "ppd_category_type": pl.Utf8,
        "record_status": pl.Utf8
    })

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
# Now we can have a lazy frame in place, we can start to build up the transformations we want apply using Polars' composable expression API:

# CELL ********************

# Convert the property_type column from single letter codes to full descriptions
price_paid_data = (
    price_paid_data
    .with_columns(
        pl.when(pl.col("property_type") == "D")
        .then(pl.lit("Detached"))
        .when(pl.col("property_type") == "S")
        .then(pl.lit("Semi-Detached"))
        .when(pl.col("property_type") == "T")
        .then(pl.lit("Terraced"))
        .when(pl.col("property_type") == "F")
        .then(pl.lit("Flat/Maisonette"))
        .when(pl.col("property_type") == "O")
        .then(pl.lit("Other"))
        .otherwise(pl.col("property_type"))
        .alias("property_type")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Do the same of old_new
price_paid_data = (
    price_paid_data
    .with_columns(
        pl.when(pl.col("old_new") == "Y")
        .then(pl.lit("New"))
        .when(pl.col("old_new") == "N")
        .then(pl.lit("Old"))
        .otherwise(pl.col("old_new"))
        .alias("old_new")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Use regex to extract the postcode area (the first one or two letters)
price_paid_data = (
    price_paid_data
    .with_columns(
        pl.col("postcode")
        .str.extract(r"^([A-Z]{1,2})", 1)
        .alias("postcode_area")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Convert date_of_transfer from datetime to date
price_paid_data = (
    price_paid_data
    .with_columns(
        pl.col("date_of_transfer")
        .dt.date()
        .alias("date_of_transfer")
    )
)

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
prices = price_paid_data.select([
    "price",
    "date_of_transfer",
    "postcode_area",
    "town_city",
    "property_type",
    "old_new",
]).collect()

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
# 
# At this stage we need to materialise the data.  But given we are operating over a single column, the operaiton will be optimised through **projection pushdown**.

# CELL ********************

min_date = prices.select(pl.col("date_of_transfer").min())[0,0]
max_date = prices.select(pl.col("date_of_transfer").max())[0,0]
min_date, max_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

dates = (
    pl.date_range(
        start=min_date,
        end=max_date,
        interval="1d",
        eager=True,
    ).
    to_frame(name="date")
    .with_columns([
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime("%B").alias("month_name"),
        pl.col("date").dt.day().alias("day"),
        pl.col("date").dt.weekday().alias("weekday"),
        pl.col("date").dt.strftime("%A").alias("weekday_name"),
        pl.col("date").dt.ordinal_day().alias("day_of_year"),
    ])
)   

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

locations = (
    price_paid_data
    .select(
        [
            "county",
            "district",
            "town_city",
        ]
    )
    .unique()
).collect()

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


# MARKDOWN ********************

# ### Write Prices

# CELL ********************

logger.info(f"Writing prices data to Parquet: {target_path_prices}")
prices.write_delta(target_path_prices, mode="overwrite", storage_options=storage_options)

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
locations.write_delta(target_path_locations, mode="overwrite", storage_options=storage_options)

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
dates.write_delta(target_path_dates, mode="overwrite", storage_options=storage_options)

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

# ## Reading from DeltaLake and generate sumamry
# 
# When we are reading delta files, we can use the Lazy execution framework to maximise scale and performance.
# 
# Let's illustrate this by doing generating some analytics in this notebook using the data we have just written to the lakehouse in Delta format.

# MARKDOWN ********************

# ### Read Prices

# CELL ********************

# Load prices from and filter them to exclude "Other" property types
prices = (
    pl.scan_delta(target_path_prices, storage_options=storage_options)
    .filter(pl.col("property_type") != "Other")
)

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

# MARKDOWN ********************

# ### Read Dates

# CELL ********************

# Load the date dimension, add a new month_tag column in the form YYYY_MM
dates = (
    pl.scan_delta(target_path_dates, storage_options=storage_options)
    .with_columns(
        [
            pl.col("date").dt.strftime("%Y_%m").alias("month_tag")
        ]
    )
)

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
prices = (
    prices
    .join(
        dates.select(
            [
                "date",
                "month_tag"
            ]
        ),
        left_on="date_of_transfer",
        right_on="date",
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Finally summarise the data up to monthly level by property type
monthly_summary = (
    prices
    .group_by(
        [
            "month_tag",
            "property_type"
        ]
    )
    .agg(
        [
            pl.len().alias("number_of_transactions"),
            pl.col("price").median().alias("median_price"),
            pl.col("price").min().alias("min_price"),
            pl.col("price").max().alias("max_price"),
        ]
    )
    .sort(
        [
            "month_tag",
            "property_type",
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

monthly_summary = monthly_summary.collect()

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

# CELL ********************

notebookutils.fs.rm(schema_path, recurse=True)
logger.info(f"Cleaned up lakehouse by everything under {schema_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
