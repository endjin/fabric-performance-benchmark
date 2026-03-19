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
# This notebook contains data preparation and analytics for the benchmarking results in Power BI.
# 
# Data is read in from Delta tables in lakehouse and the following steps are performed:
# 
# - Stage order added (to enable results to be sorted in in report).
# - Phase is added to group up stages along with phase order.
# - CUs per second are added based on the configuration of the environment which was used.
# - Stage time delta is added to compute the elapsed time between stages using a windowing function.
# 
# The data is then written out to the lakehouse.

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

# Create connection to variable library
variable_library = notebookutils.variableLibrary.getLibrary("benchmark_1_variables")

# Retrieve workspace name and lakehouse name from variable library
WORKSPACE_NAME = variable_library.workspace_name
LAKEHOUSE_NAME = variable_library.lakehouse_name

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

# ## Load raw data

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

# ## Add stages and phases reference data

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

# ## Add CUs per second based on configuration
# 
# Here we make sure the different configurations which were used across Python and Spark based environments are assigned a "CUs per second" which will be used to sort the results based on increasing scale of resources (vCores, memory, executors) and also compute a cost for running a specific workload.

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
        {
            "configuration": "02 vCores",
            "configuration_scale": "10",
            "total_v_cores": 2,
            "cu_per_second": 1,
        },
        {
            "configuration": "04 vCores",
            "configuration_scale": "20",
            "total_v_cores": 4,
            "cu_per_second": 2,
        },
        {
            "configuration": "08 vCores",
            "configuration_scale": "30",
            "total_v_cores": 8,
            "cu_per_second": 4,
        },
        {
            "configuration": "16 vCores",
            "configuration_scale": "40",
            "total_v_cores": 16,
            "cu_per_second": 8,
        },
        {
            "configuration": "32 vCores",
            "configuration_scale": "50",
            "total_v_cores": 32,
            "cu_per_second": 16,
        },
        {
            "configuration": "01 executors 04/04 cores 28g/28g memory",
            "configuration_scale": "25", 
            "total_v_cores": 8,
            "cu_per_second": 4,
        },
        {
            "configuration": "02 executors 04/04 cores 28g/28g memory", 
            "configuration_scale": "34", 
            "total_v_cores": 12,
            "cu_per_second": 6,
        },
        {
            "configuration": "01 executors 08/08 cores 56g/56g memory",
            "configuration_scale": "32",
            "total_v_cores": 16,
            "cu_per_second": 8,
        },
        {
            "configuration": "04 executors 04/04 cores 28g/28g memory",
            "configuration_scale": "46", 
            "total_v_cores": 20,
            "cu_per_second": 10,
        },
        {
            "configuration": "02 executors 08/08 cores 56g/56g memory", 
            "configuration_scale": "43", 
            "total_v_cores": 24,
            "cu_per_second": 12,
        },
        {
            "configuration": "04 executors 08/08 cores 56g/56g memory",
            "configuration_scale": "55", 
            "total_v_cores": 40,
            "cu_per_second": 20,
        },
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

configurations.write_delta(
    configurations_path,
    mode="overwrite",
    storage_options=storage_options,
    delta_write_options={"schema_mode": "overwrite"}
    )

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

# CELL ********************

benchmarks.sort("stage_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Data preparation

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

# MARKDOWN ********************

# ## Write data to lakehouse

# CELL ********************

benchmarks.write_delta(
    benchmark_analytics_path,
    mode="overwrite",
    storage_options=storage_options,
    delta_write_options={"schema_mode": "overwrite"}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
