# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
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

# CELL ********************

# Common code used to set up all notebooks
import notebookutils

WORKSPACE_NAME = "fabric_performance_benchmark_workspace"
LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"
RAW_DATA_RELATIVE_PATH = "land_registry"

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

# Common imports
import time
import logging
from datetime import datetime

# Imports unique to Pandas version of notebook
import pandas as pd
import notebookutils
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

run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

schema_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/pandas_benchmark_{run_timestamp}"

target_path_prices = f"{schema_path}/prices"
target_path_locations = f"{schema_path}/locations"
target_path_dates = f"{schema_path}/dates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

start = time.perf_counter()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

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

# ### Write tables

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

logger.info(f"Writing dates data to Parquet: {target_path_dates}")
write_deltalake(target_path_dates, dates, mode='overwrite', schema_mode='merge', engine='rust', storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Reading from DeltaLake and generate summary
# 
# Let's illustrate this by generating some analytics in this notebook using the data we have just written to the lakehouse in Delta format.

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

elapsed = time.perf_counter() - start
logger.info(f"Notebook completed in {elapsed:.2f} seconds.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
