# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run helper_methods_python

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import polars

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
schema_path = f"{base_path}/Tables/polars_experiment"

# Now construct paths to tables in that schema
target_path_monthly_summary = f"{schema_path}/monthly_summary"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create storage options
storage_options = create_storage_options()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

price_paid_data = pl.scan_csv(
    source_path,  # ABFSS path to the CSV files in the Files area.
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

price_paid_data = (
    price_paid_data
    .with_columns([
        pl.col("date_of_transfer").dt.year().alias("year"),
        pl.col("date_of_transfer").dt.month().alias("month_number"),
    ])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

monthly_summary = (
    price_paid_data
    .group_by(
        [
            "year",
            "month_number",
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
            "property_type",
            "year",
            "month_number",
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

monthly_summary.write_delta(target_path_monthly_summary, mode="overwrite", storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

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

monthly_summary.head(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import altair as alt

chart = alt.Chart(monthly_summary.filter(pl.col("year") > 2015).filter(pl.col("property_type") != "O")).mark_line().encode(
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
