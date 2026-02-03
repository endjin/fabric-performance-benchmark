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
        "bearer_token": notebookutils.credentials.getToken('storage'),
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

# CELL ********************

order_of_stages = pl.DataFrame(
    {
        "stage_name": [
            'start',
            'ingest',
            'transform',
            'write_prices',
            'write_locations',
            'write_dates',
            'read_prices',
            'read_dates',
            'join_and_summarise',
        ],
        "stage_order": [0, 1, 2, 3, 4, 5, 6, 7, 8],
        "phase": [None, "ingest_and_transform", "ingest_and_transform", "create_and_write", "create_and_write", "create_and_write", "read_and_summarise", "read_and_summarise", "read_and_summarise"],
        "phase_order": [None, 1, 1, 2, 2, 2, 3, 3, 3]
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

# CELL ********************

benchmarks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

analytics = (
    benchmarks
    .filter(pl.col("stage_name") != "start")
    .join(order_of_stages, on="stage_name")
    .group_by(["workload_name", "stage_name", "stage_order"])
    .agg(pl.col("stage_time_delta").median().alias("median_time"))
    .pivot(values="median_time", on="workload_name", index=["stage_name", "stage_order"])
    .sort("stage_order", descending=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

analytics

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

summary = (
    benchmarks
    .filter(pl.col("stage_name") != "start")
    .group_by(["workload_name", "stage_name"])
    .agg(pl.col("stage_time_delta").median().alias("median_time"))
    .group_by("workload_name")
    .agg(pl.col("median_time").sum().alias("total_median_time"))
    .sort("total_median_time", descending=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
