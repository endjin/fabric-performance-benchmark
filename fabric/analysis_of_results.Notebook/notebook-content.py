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

%pip install seaborn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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

benchmarks = f"{construct_base_abfss_path(WORKSPACE_NAME, LAKEHOUSE_NAME)}/Tables/benchmark_repository/benchmarks"

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
benchmarks = pl.read_delta(benchmarks, storage_options=storage_options)

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

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

import plotly.express as px

# Assuming 'benchmarks' dataframe is already in your notebook session
fig = px.box(
    (
        benchmarks
        .select(["workload_name", "stage_name", "stage_time_delta"])
        .filter(
            (pl.col("stage_name") != "start") &
            (pl.col("stage_name").is_in(["ingest", "create_prices", "join_and_summarise"]))
            )
    ),
    x="stage_name",
    y="stage_time_delta",
    color="workload_name",
    points="all",  # Overlay individual data points
    title="Stage Time by Stage Name and Workload Name"
)
fig.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

import matplotlib.pyplot as plt
import seaborn as sns

# Prepare data: filter and convert to pandas
selected_stages = ["ingest", "create_prices", "join_and_summarise"]
pdf = (
    benchmarks
    .select(["workload_name", "stage_name", "stage_time_delta"])
    .filter(
        (pl.col("stage_name") != "start")
        # &
        # (pl.col("stage_name").is_in(selected_stages))
    )
    .to_pandas()
)

plt.figure(figsize=(10,6))
sns.boxplot(
    data=pdf, 
    x="stage_name", 
    y="stage_time_delta", 
    hue="workload_name", 
    showfliers=True,
    palette="Set2"
)
sns.stripplot(
    data=pdf, 
    x="stage_name", 
    y="stage_time_delta", 
    hue="workload_name", 
    dodge=True,
    alpha=0.4,
    palette="Set2"
)

plt.title("Stage Time Delta by Stage Name and Workload Name")
plt.ylabel("stage_time_delta")
plt.xlabel("stage_name")
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="workload_name")
plt.tight_layout()
plt.show()

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
