# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Pyspark Benchmark
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

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import time
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
import psutil
import notebookutils
import polars as pl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger = logging.getLogger(name="pyspark_benchmark_notebook")
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Contruct source path for raw data
source_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Files/{RAW_DATA_RELATIVE_PATH}/*.csv"

# Construct base path for lakehouse schema
schema_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/pyspark_benchmark_{run_timestamp}"

# Now construct paths to tables in that schema
target_path_prices = f"{schema_path}/prices"
target_path_locations = f"{schema_path}/locations"
target_path_dates = f"{schema_path}/dates"

# Create storage options
storage_options = create_storage_options()

# Set up benchmark manager
benchmark_manager = BenchmarkManager(
    workload_name="PySpark Benchmark",
    run_timestamp=run_timestamp,
    export_abfss_path=f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmarks",
    storage_options=storage_options
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("start")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ingest Raw Data

# CELL ********************

logging.info(f"Reading price paid data from location {source_path}...")

# Define schema for CSV files
schema = StructType([
    StructField("transaction_unique_identifier", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("date_of_transfer", TimestampType(), True),
    StructField("postcode", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("old_new", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("paon", StringType(), True),
    StructField("saon", StringType(), True),
    StructField("street", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("town_city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("county", StringType(), True),
    StructField("ppd_category_type", StringType(), True),
    StructField("record_status", StringType(), True),
])

# Read CSV files - Spark can read multiple files from a directory natively
price_paid_data = (
    spark.read
    .option("header", "false")
    .option("nullValue", "")
    .schema(schema)
    .csv(source_path)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("ingest")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Transformation
# 
# Now we have the DataFrame loaded, we can start to build up the transformations we want to apply using PySpark's DataFrame API:

# CELL ********************

# Convert the property_type column from single letter codes to full descriptions
price_paid_data = (
    price_paid_data
    .withColumn(
        "property_type",
        F.when(F.col("property_type") == "D", F.lit("Detached"))
        .when(F.col("property_type") == "S", F.lit("Semi-Detached"))
        .when(F.col("property_type") == "T", F.lit("Terraced"))
        .when(F.col("property_type") == "F", F.lit("Flat/Maisonette"))
        .when(F.col("property_type") == "O", F.lit("Other"))
        .otherwise(F.col("property_type"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Do the same for old_new
price_paid_data = (
    price_paid_data
    .withColumn(
        "old_new",
        F.when(F.col("old_new") == "Y", F.lit("New"))
        .when(F.col("old_new") == "N", F.lit("Old"))
        .otherwise(F.col("old_new"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use regex to extract the postcode area (the first one or two letters)
price_paid_data = (
    price_paid_data
    .withColumn(
        "postcode_area",
        F.regexp_extract(F.col("postcode"), r"^([A-Z]{1,2})", 1)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert date_of_transfer from timestamp to date
price_paid_data = (
    price_paid_data
    .withColumn(
        "date_of_transfer",
        F.to_date(F.col("date_of_transfer"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("transform")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create fact table
# 
# Select the core columns we want to use in the core fact table.

# CELL ********************

# Select relevant columns for downstream analysis
prices = price_paid_data.select(
    "price",
    "date_of_transfer",
    "postcode_area",
    "town_city",
    "property_type",
    "old_new",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("create_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create date dimension
# 
# Use min and max dates to build date dimension table.
# 
# Spark uses lazy evaluation, so the computation is deferred until an action is triggered.

# CELL ********************

date_stats = price_paid_data.agg(
    F.min("date_of_transfer").alias("min_date"),
    F.max("date_of_transfer").alias("max_date")
).collect()[0]

min_date = date_stats["min_date"]
max_date = date_stats["max_date"]
min_date, max_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate date range using sequence function
dates = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{min_date}'),
            to_date('{max_date}'),
            interval 1 day
        )) as date
    """)
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("weekday", F.dayofweek("date") - 1)  # Adjust to 0-based (Monday=0)
    .withColumn("weekday_name", F.date_format("date", "EEEE"))
    .withColumn("day_of_year", F.dayofyear("date"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("create_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
        "county",
        "district",
        "town_city",
    )
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("create_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Writing to Delta Tables
# 
# It is common practice to write out a PySpark DataFrame to a Delta table in the Tables area of your Lakehouse.
# 
# There are various write modes which are available:
# 
# Overwrite entire table:
# 
# ```python
# df.write.format("delta").mode("overwrite").save(path)
# ```
# 
# Append to existing table:
# 
# ```python
# df.write.format("delta").mode("append").save(path)
# ```
# 
# Merge (upsert) - use DeltaTable API:
# 
# ```python
# from delta.tables import DeltaTable
# 
# delta_table = DeltaTable.forPath(spark, path)
# (
#     delta_table.alias("target")
#     .merge(
#         df.alias("source"),
#         "source.id = target.id"
#     )
#     .whenMatchedUpdateAll()
#     .whenNotMatchedInsertAll()
#     .execute()
# )
# ```

# MARKDOWN ********************

# ### Handling Timestamps
# 
# A common gotcha when writing Delta tables is timezone handling. Fabric's SQL endpoint expects timestamps with timezone information.
# 
# We can address this by converting to UTC timestamp, for example:
# 
# ```python
# df = df.withColumn(
#     "datetime_of_order",
#     F.to_utc_timestamp(F.col("datetime_of_order"), "UTC")
# )
# ```

# MARKDOWN ********************

# ### Write Prices

# CELL ********************

import os
logger.info(f"Writing prices data to Parquet: {target_path_prices}")
prices.write.mode("overwrite").format("delta").save(target_path_prices)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("write_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Locations

# CELL ********************

logger.info(f"Writing locations data to Parquet: {target_path_locations}")
locations.write.mode("overwrite").format("delta").save(target_path_locations)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("write_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write Dates

# CELL ********************

logger.info(f"Writing dates data to Parquet: {target_path_dates}")
dates.write.mode("overwrite").format("delta").save(target_path_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("write_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reading from DeltaLake and generate summary
# 
# Spark uses lazy evaluation by default, so transformations are not executed until an action is triggered.
# 
# Let's illustrate this by generating some analytics in this notebook using the data we have just written to the lakehouse in Delta format.

# MARKDOWN ********************

# ### Read Prices

# CELL ********************

# Load prices from Parquet and filter them to exclude "Other" property types
logger.info(f"Reading prices data back from Parquet: {target_path_prices}")
prices = (
    spark.read
    .format("delta")
    .load(target_path_prices)
    .filter(F.col("property_type") != "Other")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("read_prices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read Dates

# CELL ********************

# Load the date dimension, add a new month_tag column in the form YYYY_MM
logger.info(f"Reading dates data back from Parquet: {target_path_dates}")
dates = (
    spark.read
    .format("delta")
    .load(target_path_dates)
    .withColumn(
        "month_tag",
        F.date_format("date", "yyyy_MM")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("read_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Join and Summarise

# CELL ********************

# Now join the two tables to get month_tag into the prices table
prices = (
    prices
    .join(
        dates.select("date", "month_tag"),
        prices["date_of_transfer"] == dates["date"],
        how="left"
    )
    .drop(dates["date"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Finally summarise the data up to monthly level by property type
monthly_summary = (
    prices
    .groupBy("month_tag", "property_type")
    .agg(
        F.count("*").alias("number_of_transactions"),
        F.percentile_approx("price", 0.5).alias("median_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
    )
    .orderBy("month_tag", "property_type")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

monthly_summary.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("join_and_summarise")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_results = benchmark_manager.export_results()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

elapsed = benchmark_results["stage_time"].max() - benchmark_results["stage_time"].min()
logger.info(f"Notebook completed in {elapsed:.2f} seconds.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
