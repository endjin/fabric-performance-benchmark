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

# # Analysis of Results
# 
# This notebook contains data preparation and analytics for the benchmarking results.

# MARKDOWN ********************

# ## Set Up

# CELL ********************

import polars as pl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Pre-requisities are to create a Fabric Workspace with a lakehouse, putting names here:
WORKSPACE_NAME = "fabric_performance_benchmark_workspace"
LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

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
        "bearer_token": notebookutils.credentials.getToken("storage"),
        "use_fabric_endpoint": "true"
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmarks"

stages_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/stages"

configurations_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/configurations"

benchmark_analytics_path = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmark_analytics"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

storage_options = create_storage_options()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load and Prepare Raw Data

# CELL ********************

# Load prices from and filter them to exclude "Other" property types
benchmarks = pl.read_delta(benchmarks_path, storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks.schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Create Stages Reference Data

# CELL ********************

order_of_stages = pl.DataFrame(
    {
        "stage_name": [
            "start",
            "setup",
            "ingest",
            "transform",
            "write_prices",
            "write_locations",
            "write_dates",
            "read_prices",
            "read_dates",
            "join_and_summarise",
        ],
        "stage_order": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "phase": ["start_and_setup", "start_and_setup", "ingest_and_transform", "ingest_and_transform", "create_and_write", "create_and_write", "create_and_write", "read_and_summarise", "read_and_summarise", "read_and_summarise"],
        "phase_order": [1, 1, 2, 2, 3, 3, 3, 4, 4, 4]
    }
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

order_of_stages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

order_of_stages.write_delta(stages_path, mode="overwrite", storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks = benchmarks.join(order_of_stages, on="stage_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Configuration Scale
# 
# Here we make sure the different configuations which were used across Python and Spark based environments are given an "configuration_scale" which will be used to sort the results based on increasing scale of resources (vCores, memory, executors).

# CELL ********************

benchmarks["configuration"].unique().to_list()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

configurations = pl.DataFrame(
    [
        {"configuration": "02 vCores", "configuration_scale": "10", "t_shirt_size": "XS"},
        {"configuration": "04 vCores", "configuration_scale": "20", "t_shirt_size": "S"},
        {"configuration": "08 vCores", "configuration_scale": "30", "t_shirt_size": "M"},
        {"configuration": "16 vCores", "configuration_scale": "40", "t_shirt_size": "L"},
        {"configuration": "32 vCores", "configuration_scale": "50", "t_shirt_size": "XL"},
        {"configuration": "01 executors 04/04 cores 28g/28g memory", "configuration_scale": "25", "t_shirt_size": "S"},
        {"configuration": "01 executors 08/08 cores 56g/56g memory", "configuration_scale": "32", "t_shirt_size": "M"},
        {"configuration": "02 executors 04/04 cores 28g/28g memory", "configuration_scale": "34", "t_shirt_size": "M"},
        {"configuration": "02 executors 08/08 cores 56g/56g memory", "configuration_scale": "43", "t_shirt_size": "L"},
        {"configuration": "04 executors 04/04 cores 28g/28g memory", "configuration_scale": "46", "t_shirt_size": "L"},
        {"configuration": "04 executors 08/08 cores 56g/56g memory", "configuration_scale": "55", "t_shirt_size": "XL"},
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

configurations.write_delta(configurations_path, mode="overwrite", storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks = benchmarks.join(configurations, on="configuration")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Data Preparation

# CELL ********************

benchmarks = (
    benchmarks
    .sort(["run_timestamp", "order"])
    .with_columns(
        pl.col("stage_time_delta")
        .cum_sum()
        .over(["platform", "configuration", "workload_name", "run_timestamp"])
        .alias("cumulative_time")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

benchmarks.write_delta(benchmark_analytics_path, mode="overwrite", storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

stage_benchmarks = (
    benchmarks
    .sort(["run_timestamp", "order"])
    .group_by(["platform", "configuration", "workload_name", "run_timestamp", "phase", "phase_order", "configuration_scale"])
    .agg(pl.col("stage_time_delta").sum().alias("phase_time"))
    .sort(["platform", "configuration", "workload_name", "run_timestamp", "phase_order"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

stage_benchmarks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

overall_benchmarks = (
    benchmarks
    .sort(["run_timestamp", "order"])
    .group_by(["platform", "configuration", "workload_name", "run_timestamp", "configuration_scale"])
    .agg(pl.col("stage_time_delta").sum().alias("total_time"))
    .sort(["platform", "configuration", "workload_name", "run_timestamp"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

overall_benchmarks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
