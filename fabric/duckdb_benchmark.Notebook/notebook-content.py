# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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
from dataclasses import dataclass, asdict
import psutil
import json
import notebookutils

# Imports specific to duckdb version of notebook
import duckdb
import polars as pl
from deltalake import write_deltalake  # Unfortunately duckdb does not yet support writing to Azure, so we need write_deltalake to address that requirement

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Common Code
# 
# The code in this section is common across all notebooks.
# 
# It is used to:
# - Set up logging
# - Set constants for workspace and lakehouse names
# - Set relative path for source data used as input
# - Set the ABFSS paths for reading from / writing to lakehouse
# - Set up the `storage_options` parameter
# - Log benchmarks

# CELL ********************

logger = logging.getLogger(name="duckdb_benchmark_notebook")
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

# Helper function to create storage options that enable data tools to authenticate and interact with onelake storage
def create_storage_options() -> dict:
    return {
        "bearer_token": notebookutils.credentials.getToken('storage'),
        "use_fabric_endpoint": "true"
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
    cpu_count:float
    cpu_usage: float
    memory: float
    memory_usage:float


# Benchmark Manager class will help capture benchmarks consistently, then write them out to lakehouse at end of notebook
class BenchmarkManager:

    benchmarks = []

    def __init__(self, platform: str, configuration:str, workload_name: str, run_timestamp: str, export_abfss_path:str, storage_options:dict):
        self.platform = platform
        self.configuration = configuration
        self.workload_name = workload_name
        self.run_timestamp = run_timestamp
        self.export_abfss_path = export_abfss_path
        self.storage_options = storage_options
    
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
        records_to_export = pl.DataFrame([asdict(benchmark) for benchmark in self.benchmarks])
        records_to_export = (
            records_to_export
            .sort("stage_time", descending=False)
            .with_row_index("order", offset=1)
            .with_columns((pl.col("stage_time").diff().dt.total_milliseconds().alias("stage_time_delta") / 1000))
        )
        records_to_export.write_delta(self.export_abfss_path, mode="append", storage_options=self.storage_options)
        return records_to_export


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Configuration
# 
# Configuring the lakehouse paths and helper functions used throughout the notebook.

# CELL ********************

# Contruct source path for raw data
source_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Files/{RAW_DATA_RELATIVE_PATH}/*.csv"

# Construct base path for lakehouse schema
schema_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/duckdb_benchmark_{run_timestamp}"

# Now construct paths to tables in that schema
target_path_prices = f"{schema_path}/prices"
target_path_locations = f"{schema_path}/locations"
target_path_dates = f"{schema_path}/dates"

# Create storage options
storage_options = create_storage_options()

# Set up benchmark manager
benchmark_manager = BenchmarkManager(
    platform="Fabric Python Notebook",
    configuration=f"{v_cores} vCores",
    workload_name=notebook,
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

# Create an in-memory DuckDB connection
con = duckdb.connect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

duckdb.__version__

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Install and load Delta Lake extension for reading/writing Delta tables
con.execute("INSTALL delta;")
con.execute("LOAD delta;")
con.execute("INSTALL azure")
con.execute("LOAD azure;")

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

# ### Ingest Raw Data

# CELL ********************

logging.info(f"Reading price paid data from location {source_path}...")

# DuckDB can read multiple CSV files using glob patterns
# Create a view from the CSV files with explicit column names
con.execute(f"""
    CREATE OR REPLACE VIEW price_paid_raw AS
    SELECT 
        column00 AS transaction_unique_identifier,
        CAST(column01 AS DOUBLE) AS price,
        CAST(column02 AS TIMESTAMP) AS date_of_transfer,
        column03 AS postcode,
        column04 AS property_type,
        column05 AS old_new,
        column06 AS duration,
        column07 AS paon,
        column08 AS saon,
        column09 AS street,
        column10 AS locality,
        column11 AS town_city,
        column12 AS district,
        column13 AS county,
        column14 AS ppd_category_type,
        column15 AS record_status
    FROM read_csv(
        '{source_path}',
        header=false,
        nullstr=''
    )
""")

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

# ### Data Transformation
# 
# With DuckDB, we use SQL to transform our data. Views allow us to build up transformations lazily - the actual computation happens when we query the final result.

# CELL ********************

# Apply all transformations in a single SQL statement:
# - Convert property_type codes to full descriptions
# - Convert old_new codes to full descriptions  
# - Extract postcode area using regex
# - Convert date_of_transfer to date type

con.execute("""
    CREATE OR REPLACE TABLE price_paid_data AS
    SELECT
        transaction_unique_identifier,
        price,
        CAST(date_of_transfer AS DATE) AS date_of_transfer,
        postcode,
        CASE property_type
            WHEN 'D' THEN 'Detached'
            WHEN 'S' THEN 'Semi-Detached'
            WHEN 'T' THEN 'Terraced'
            WHEN 'F' THEN 'Flat/Maisonette'
            WHEN 'O' THEN 'Other'
            ELSE property_type
        END AS property_type,
        CASE old_new
            WHEN 'Y' THEN 'New'
            WHEN 'N' THEN 'Old'
            ELSE old_new
        END AS old_new,
        town_city,
        district,
        county,
        regexp_extract(postcode, '^([A-Z]{1,2})', 1) AS postcode_area
    FROM price_paid_raw
""")

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

# ### Create and write Prices fact table
# 
# Select the core columns we want to use in the core fact table.

# CELL ********************

# Create prices view with selected columns
con.execute("""
    CREATE OR REPLACE VIEW prices AS
    SELECT
        price,
        date_of_transfer,
        postcode_area,
        town_city,
        property_type,
        old_new
    FROM price_paid_data
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Writing prices data to Parquet: {target_path_prices}")
write_deltalake(
    target_path_prices,
    con.execute("SELECT * FROM prices").fetch_record_batch(),
    mode='overwrite', 
    engine='rust',
    storage_options=storage_options
)

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

# ### Create and write Dates dimension
# 
# Use min and max dates to build date dimension table.
# 
# DuckDB's `generate_series` function makes it easy to create date ranges.

# CELL ********************

# Get min and max dates
date_range = con.execute("""
    SELECT 
        MIN(date_of_transfer) AS min_date,
        MAX(date_of_transfer) AS max_date
    FROM price_paid_data
""").fetchone()

min_date, max_date = date_range
min_date, max_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create date dimension table using generate_series
con.execute(f"""
    CREATE OR REPLACE VIEW dates AS
    SELECT
        date::DATE AS date,
        EXTRACT(YEAR FROM date)::INTEGER AS year,
        EXTRACT(MONTH FROM date)::INTEGER AS month,
        strftime(date, '%B') AS month_name,
        EXTRACT(DAY FROM date)::INTEGER AS day,
        EXTRACT(DAYOFWEEK FROM date)::INTEGER AS weekday,
        strftime(date, '%A') AS weekday_name,
        EXTRACT(DAYOFYEAR FROM date)::INTEGER AS day_of_year
    FROM generate_series(
        DATE '{min_date}',
        DATE '{max_date}',
        INTERVAL 1 DAY
    ) AS t(date)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Writing dates data to Parquet: {target_path_dates}")
write_deltalake(
    target_path_dates,
    con.execute("SELECT * FROM dates").arrow(),
    mode='overwrite',
    engine='rust',
    storage_options=storage_options
)

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
# Assumption is there is a hierarchy in decreasing order of granularity:
# 
# - County
# - District
# - Town or City

# CELL ********************

# Create locations view with unique combinations
con.execute("""
    CREATE OR REPLACE VIEW locations AS
    SELECT DISTINCT
        county,
        district,
        town_city
    FROM price_paid_data
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

logger.info(f"Writing locations data to Parquet: {target_path_locations}")
write_deltalake(
    target_path_locations,
    con.execute("SELECT * FROM locations").arrow(),
    mode='overwrite',
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

# ## Phase 3 - Read and Summarise
# 
# DuckDB can read Delta tables directly using the `delta_scan` function.
# 
# Let's generate some analytics using the data we have just written.

# MARKDOWN ********************

# ### Read Prices

# CELL ********************

# Load prices from Parquet and filter out "Other" property types
logger.info(f"Reading prices data back from Parquet: {target_path_prices}")
con.execute(f"""
    CREATE OR REPLACE VIEW prices_filtered AS
    SELECT *
    FROM delta_scan('{target_path_prices}')
    WHERE property_type != 'Other'
""")

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

# Load the date dimension with month_tag column
logger.info(f"Reading dates data back from Parquet: {target_path_dates}")
con.execute(f"""
    CREATE OR REPLACE VIEW dates_with_tag AS
    SELECT 
        *,
        strftime(date, '%Y_%m') AS month_tag
    FROM delta_scan('{target_path_dates}')
""")

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

# Join prices with dates and create monthly summary in one query
# This demonstrates DuckDB's ability to compose complex queries
monthly_summary = con.execute("""
    SELECT
        d.month_tag,
        p.property_type,
        COUNT(*) AS number_of_transactions,
        MEDIAN(p.price) AS median_price,
        MIN(p.price) AS min_price,
        MAX(p.price) AS max_price
    FROM prices_filtered p
    LEFT JOIN dates_with_tag d
        ON p.date_of_transfer = d.date
    GROUP BY
        d.month_tag,
        p.property_type
    ORDER BY
        d.month_tag,
        p.property_type
""").pl()

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
logger.info(f"Notebook completed in {elapsed}.")

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

# CELL ********************

# Clean up - close the connection
con.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
