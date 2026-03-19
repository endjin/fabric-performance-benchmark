# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC { 
# MAGIC     "driverMemory": 
# MAGIC     { 
# MAGIC         "parameterName": "driver_memory", 
# MAGIC         "defaultValue": "56g" 
# MAGIC     },
# MAGIC     "driverCores": 
# MAGIC     { 
# MAGIC         "parameterName": "driver_cores", 
# MAGIC         "defaultValue": 8
# MAGIC     },
# MAGIC     "executorMemory": 
# MAGIC     { 
# MAGIC         "parameterName": "executor_memory", 
# MAGIC         "defaultValue": "56g"
# MAGIC     },
# MAGIC     "executorCores": 
# MAGIC     { 
# MAGIC         "parameterName": "executor_cores", 
# MAGIC         "defaultValue": 8 
# MAGIC     },
# MAGIC     "numExecutors": 
# MAGIC     { 
# MAGIC         "parameterName": "executor_number", 
# MAGIC         "defaultValue": 1 
# MAGIC     },    
# MAGIC } 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

run_timestamp = None
notebook = ""
driver_memory = "56g"
driver_cores = 8
executor_memory = "56g"
executor_cores = 8
executor_number = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_context = spark.sparkContext
configuration = spark_context.getConf()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    configuration.get("spark.driver.memory"),
    configuration.get("spark.driver.cores"),
    configuration.get("spark.executor.memory"),
    configuration.get("spark.executor.cores"),
    configuration.get("spark.executor.instances"),
    configuration.get("spark.dynamicAllocation.enabled")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# ## Phase 0 - Set up

# CELL ********************

# Common imports
import time
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
import psutil
import json
import notebookutils
from typing import Callable, Any
from urllib.parse import quote

# Imports unique to PySpark version of notebook
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def export_with_spark(manager):
    """Export benchmark results using PySpark and write to Delta Lake."""

    records = spark.createDataFrame([asdict(b) for b in manager.benchmarks])

    window_spec = Window.orderBy("stage_time")

    records = (
        records.orderBy("stage_time")
        .withColumn("order", F.row_number().over(window_spec))
        .withColumn(
            "stage_time_delta",
            F.round(
                F.col("stage_time").cast("double")
                - F.lag("stage_time", 1).over(window_spec).cast("double"),
                3,
            ),
        )
        .withColumn("stage_time", F.col("stage_time").cast("timestamp_ntz"))
    )

    records.write.format("delta").mode("append").save(manager.export_abfss_path)
    return records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This code is common across both Python and Polars notebooks, but we need to duplicate it across platforms.

# Helper function to create base ABFSS path based on workspace and lakehouse name
def construct_base_abfss_path(workspace_name: str, lakehouse_name: str) -> str:
    """Construct the base ABFSS path for a given workspace and lakehouse."""
    workspace_name = quote(workspace_name, safe="")
    lakehouse_name = quote(lakehouse_name, safe="")
    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"


# Helper function to create storage options that enable data tools to authenticate and interact with onelake storage
def create_storage_options() -> dict:
    return {
        "bearer_token": notebookutils.credentials.getToken("storage"),
        "use_fabric_endpoint": "true",
    }


# Data class to store benchmark metrics at key points during notebook process
@dataclass
class Benchmark:
    platform: str
    configuration: str
    workload_name: str
    run_timestamp: str
    stage_name: str
    stage_time: float
    cpu_count: float
    cpu_usage: float
    memory: float
    memory_usage: float


# Benchmark Manager class will help capture benchmarks consistently, then write them out to lakehouse at end of notebook
class BenchmarkManager:

    def __init__(
        self,
        platform: str,
        configuration: str,
        workload_name: str,
        run_timestamp: str,
        export_abfss_path: str,
        storage_options: dict,
        exporter: Callable[["BenchmarkManager"], Any],
    ):
        self.platform = platform
        self.configuration = configuration
        self.workload_name = workload_name
        self.run_timestamp = run_timestamp
        self.export_abfss_path = export_abfss_path
        self.storage_options = storage_options
        self.benchmarks = []
        self._exporter = exporter

    def capture_benchmark(self, stage_name, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()

        self.benchmarks.append(
            Benchmark(
                platform=self.platform,
                configuration=self.configuration,
                workload_name=self.workload_name,
                run_timestamp=self.run_timestamp,
                stage_name=stage_name,
                stage_time=timestamp,
                cpu_count=psutil.cpu_count(),
                cpu_usage=psutil.cpu_percent(interval=None),
                memory=(psutil.virtual_memory().total / (1024 * 1024 * 1024)),
                memory_usage=psutil.virtual_memory().percent,
            )
        )

    def export_results(self):
        return self._exporter(self)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# In this section we perform the following tasks:
# - Set up logging
# - Set constants for workspace and lakehouse names
# - Set relative path for source data used as input
# - Set the ABFSS paths for reading from / writing to lakehouse
# - Set up the `storage_options` parameter
# - Log initial benchmarks

# CELL ********************

logger = logging.getLogger(name="pyspark_benchmark_notebook")
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not run_timestamp:
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"No timestamp set, using: {run_timestamp}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Construct source path for raw data
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
    platform="Fabric PySpark Notebook",
    configuration=f"{executor_number:02} executors {driver_cores:02}/{executor_cores:02} cores {driver_memory}/{executor_memory} memory",
    workload_name=notebook,
    run_timestamp=run_timestamp,
    export_abfss_path=f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmarks",
    storage_options=storage_options,
    exporter=export_with_spark
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert from string in format "yyyyMMdd_HHmmss" to datetime
benchmark_manager.capture_benchmark("start", timestamp=datetime.strptime(run_timestamp, '%Y%m%d_%H%M%S'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

benchmark_manager.capture_benchmark("setup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Phase 1 - Ingest and Transform Raw Data

# MARKDOWN ********************

# ### Ingest Raw Data

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

# Select only the columns we want to use and therefore cache
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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Cache data
# 
# Cache the transformed data to optimise downstream data processing.

# CELL ********************

price_paid_data_cached = price_paid_data.cache()

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

# ## Phase 2 - Create and write dimensional model

# MARKDOWN ********************

# ### Create and write Prices fact table
# 
# Select the core columns we want to use in the core fact table.

# CELL ********************

# Select relevant columns for downstream analysis
prices = price_paid_data_cached.select(
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

logger.info(f"Writing prices data to Delta: {target_path_prices}")
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

# ### Create date dimension
# 
# Use min and max dates to build date dimension table.
# 
# Spark uses lazy evaluation, so the computation is deferred until an action is triggered.

# CELL ********************

date_stats = price_paid_data_cached.agg(
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

logger.info(f"Writing dates data to Delta: {target_path_dates}")
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

# ### Create and write Locations dimension
# 
# Assumption is there is a hierarchy in decreasing order of granularity:
# 
# - County
# - District
# - Town or City

# CELL ********************

locations = (
    price_paid_data_cached
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

logger.info(f"Writing locations data to Delta: {target_path_locations}")
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

# ## Phase 3 - Read and Summarise
# 
# Spark uses lazy evaluation by default, so transformations are not executed until an action is triggered.
# 
# Let's illustrate this by generating some analytics in this notebook using the data we have just written to the lakehouse in Delta format.

# MARKDOWN ********************

# ### Read Prices

# CELL ********************

# Load prices from Delta and filter them to exclude "Other" property types
logger.info(f"Reading prices data back from Delta: {target_path_prices}")
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
logger.info(f"Reading dates data back from Delta: {target_path_dates}")
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

benchmark_results.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

elapsed = benchmark_results.select(
    (F.max("stage_time") - F.min("stage_time")).alias("elapsed")
).collect()[0]["elapsed"]
logger.info(f"Notebook completed in {elapsed}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.rm(schema_path, recurse=True)
logger.info(f"Cleaned up lakehouse by everything under {schema_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
