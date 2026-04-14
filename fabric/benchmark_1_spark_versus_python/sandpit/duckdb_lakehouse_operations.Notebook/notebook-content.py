# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Notebook Summary: DuckDB & Lakehouse Operations
# 
# This notebook demonstrates how to process property transaction data using DuckDB and Delta Lake integration in a Microsoft Fabric Lakehouse environment. Key steps and components include:
# 
# - **Initialization**: Loads helper methods, necessary libraries (`duckdb`, `deltalake`, `polars`, etc.), and sets up key paths and constants for workspace, lakehouse, and raw data locations.
# - **Path Construction**: Constructs source (CSV) and target (Delta table) paths leveraging Lakehouse and workspace naming conventions.
# - **Data Loading and Transformation**:
#   - Loads raw CSV transaction data into an in-memory DuckDB view.
#   - Applies column renaming, type casting, and adds derived fields (`month_number`, `year`).
#   - Provides a sample preview of the cleaned/renamed data.
# - **Aggregation**:
#   - Creates a second DuckDB view computing monthly summaries by `year`, `month_number`, and `property_type` (count, median, min, max of `price`).
#   - Queries and filters this summary view, loading results into a Polars DataFrame.
# - **Visualization**: Plots median price trends by property type and time using Altair.
# - **Writing to Delta Lake**:
#   - Uses the `deltalake` library to write the summary results as a Delta Lake table directly to the Lakehouse tables path, making data performant and queryable across Fabric.
# 
# _This notebook provides a reference workflow for efficient ETL, summarization, and delta-format data publishing within the Fabric data platform._


# CELL ********************

%run helper_methods_python

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import duckdb
from deltalake import write_deltalake  # Unfortunately duckdb does not yet support writing to Azure, so we need write_deltalake to address that requirement

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

WORKSPACE_NAME = "fabric_performance_benchmark_workspace"
LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"
RAW_DATA_RELATIVE_PATH = "land_registry"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

base_path = construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)
base_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Construct source path for raw data
source_path = f"{base_path}/Files/{RAW_DATA_RELATIVE_PATH}/*.csv"

# Construct base path for lakehouse schema
schema_path = f"{base_path}/Tables/duckdb_experiment"

# Now construct paths to tables in that schema
target_path_monthly_summary = f"{schema_path}/monthly_summary"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create an in-memory DuckDB connection
duckdb_connection = duckdb.connect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Install and load Delta Lake extension for reading/writing Delta tables
duckdb_connection.execute("INSTALL delta;")
duckdb_connection.execute("LOAD delta;")
duckdb_connection.execute("INSTALL azure")
duckdb_connection.execute("LOAD azure;")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

duckdb_connection.execute(f"""
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
        column15 AS record_status,
        EXTRACT('month' FROM CAST(column02 AS TIMESTAMP)) AS month_number,
        EXTRACT('year' FROM CAST(column02 AS TIMESTAMP)) AS year
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

duckdb_connection.query("SELECT transaction_unique_identifier, price, postcode, property_type, record_status, month_number, year FROM price_paid_raw LIMIT 5") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

duckdb_connection.execute("""
    CREATE OR REPLACE VIEW monthly_summaries AS
    SELECT
        year,
        month_number,
        property_type,
        COUNT(*) AS number_of_transactions,
        MEDIAN(price) AS median_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price
    FROM price_paid_raw
    GROUP BY
        year,
        month_number,
        property_type
    ORDER BY
        property_type,
        year,
        month_number
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

monthly_summary = duckdb_connection.query("SELECT * FROM monthly_summaries WHERE year > 2015 AND property_type != 'O'").pl()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

monthly_summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import polars as pl

monthly_summary = monthly_summary.with_columns(
    pl.concat_str([
        pl.col("year").cast(str),
        pl.col("month_number").cast(str).str.zfill(2)
    ], separator="_").alias("year_month")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import altair as alt

chart = alt.Chart(monthly_summary).mark_line().encode(
    x=alt.X("year_month:N", title="Year_Month"),
    y=alt.Y("median_price:Q", title="Median Price"),
    color=alt.Color("property_type:N", title="Property Type"),
    tooltip=[
        "year_month:N",
        "property_type:N",
        "median_price:Q",
        "number_of_transactions:Q",
        "min_price:Q",
        "max_price:Q",
    ]
).properties(
    width=1600,
    height=600,
    title="Median Price by Property Type Over Time"
)

chart

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create storage options
storage_options = create_storage_options()

write_deltalake(
    target_path_monthly_summary,
    duckdb_connection.execute("SELECT * FROM monthly_summaries").arrow(),
    mode='overwrite',
    engine='rust',
    storage_options=storage_options
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
