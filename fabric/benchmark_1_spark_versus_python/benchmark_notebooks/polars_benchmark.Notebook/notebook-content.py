# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "26fa5d18-b3bc-49f6-be0d-3424cd026fce",
# META       "default_lakehouse_name": "fabric_performance_benchmark_lakehouse",
# META       "default_lakehouse_workspace_id": "23d2362b-6b5f-4894-8472-c09991fc07a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "26fa5d18-b3bc-49f6-be0d-3424cd026fce"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC { 
# MAGIC     "vCores": 
# MAGIC     { 
# MAGIC         "parameterName": "v_cores", 
# MAGIC         "defaultValue": 2 
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

run_timestamp = None
v_cores = 2
notebook = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%run helper_methods

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%run export_benchmarks_python

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Polars Benchmark
# 
# This notebook runs a representative end to end use case over data sourced from the [UK Land Registry House Price Data open data repository](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads).
# 
# This data is made available for us under an [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
# 
# After set up (Phase 0) - we will run three phases:
# 
# 1. Load raw data, clean it up, add new features
# 1. Using the output of the phase above, create and write a 3 tables (prices, locations, dates) to the lakehouse.
# 1. Run a query across two of the tables above by joining them and summarising the data.

# MARKDOWN ********************

# ## Phase 0 - Set up

# CELL ********************

# Common imports
import time
import logging
from datetime import datetime, timezone

# Imports unique to Polars version of notebook
import polars as pl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# In this section we perform the following tasks:
# - Set up logging
# - Set constants for workspace and lakehouse names
# - Set relative path for source data used as input
# - Set the ABFSS paths for reading from / writing to lakehouse
# - Set up the `storage_options` parameter
# - Log initial benchmarks

# MARKDOWN ********************


# CELL ********************

logger = logging.getLogger(name="polars_benchmark_notebook")
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if not run_timestamp:
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"No timestamp set, using: {run_timestamp}")

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
    platform="Fabric Python Notebook",
    configuration=f"{v_cores:02} vCores",
    workload_name=notebook,
    run_timestamp=run_timestamp,
    export_abfss_path=f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmarks",
    storage_options=storage_options,
    exporter=export_with_polars
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Convert from string in formant "yyyyMMdd_HHmmss" to datetime
benchmark_manager.capture_benchmark("start", timestamp=datetime.strptime(run_timestamp, '%Y%m%d_%H%M%S'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("setup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Phase 1 - Ingest and Transform Raw Data

# MARKDOWN ********************

# ### Ingest Data

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

# ### Transformation
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

price_paid_data = (
    price_paid_data
    .select(
        [
            "price",
            "date_of_transfer",
            "postcode",
            "postcode_area",
            "property_type",
            "old_new",
            "town_city",
            "district",
            "county",
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Cache
# 
# Here we use the `collect()` method to cache the transformed raw data so that downstream creation of Prices, Dates and Locations can leverage a single source of data in memory, rather than re-loading it multuple times from storage.

# CELL ********************

# Cache the transformed data for downstream usage
price_paid_data_cached = price_paid_data.collect().lazy()

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

# ## Phase 2 - Create and write dimensional model

# MARKDOWN ********************

# ### Create and write Prices table
# 
# Select the core columns we want to use in the core fact table and write these to the lakehouse.

# CELL ********************

# Select relevant columns for downstream analysis
prices = price_paid_data_cached.select([
    "price",
    "date_of_transfer",
    "postcode_area",
    "town_city",
    "property_type",
    "old_new",
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Writing prices data to Parquet: {target_path_prices}")
prices.collect().write_delta(target_path_prices, mode="overwrite", storage_options=storage_options)

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

# ### Create and write Date dimension
# 
# Use min and max dates to build date dimension table.

# CELL ********************

min_date = price_paid_data_cached.select(pl.col("date_of_transfer").min()).collect()[0,0]
max_date = price_paid_data_cached.select(pl.col("date_of_transfer").max()).collect()[0,0]
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

# ### Create and write Locations dimension
# 
# Assumption is there is a hierarchy in descreasing order of granularity:
# 
# - County
# - District
# - Town or City

# CELL ********************

locations = (
    price_paid_data_cached
    .select(
        [
            "county",
            "district",
            "town_city",
        ]
    )
    .unique()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Writing locations data to Parquet: {target_path_locations}")
locations.collect().write_delta(target_path_locations, mode="overwrite", storage_options=storage_options)

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

# ## Phase 3 - Read and Summarise
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

# MARKDOWN ********************

# ## Phase 4 - Capture Results and Clean Up

# MARKDOWN ********************

# ### Write Benchmark Results to Lakehouse

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
logger.info(f"Notebook completed in {elapsed}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Clean Up Lakehouse

# CELL ********************

notebookutils.fs.rm(schema_path, recurse=True)
logger.info(f"Cleaned up lakehouse by everything under {schema_path}")

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
