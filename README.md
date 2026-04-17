# Fabric Performance Benchmark

A benchmarking framework for comparing data processing engines on Microsoft Fabric. This project evaluates **Pandas**, **PySpark**, **Polars**, and **DuckDB** across various compute configurations to provide concrete, Fabric-specific evidence for choosing the right engine for your workloads.

## Key Findings

Our benchmarking reveals that for medium-scale datasets (up to ~100GB):

- **DuckDB and Polars** on Python Notebooks consistently outperform PySpark, often by 2x or more
- **Cost efficiency** strongly favours Python Notebooks — the cheapest Spark configuration costs 4-5x more than equivalent DuckDB runs
- The **default Python Notebook** (2 vCores) is 8x cheaper per second than the default Spark Notebook
- There's a **"sweet spot"** for resource allocation — more infrastructure doesn't always mean faster execution

For detailed analysis, see our blog post: [Fabric Performance Benchmarking](https://endjin.com/blog/2026/03/fabric-performance-benchmarking-part-1).

## Prerequisites

Before you begin, ensure you have:

1. **GitHub Account** — to fork the repository and enable Git integration with Fabric
2. **Microsoft Fabric Capacity** — an active Fabric capacity (**F32 or above**) with permissions to create workspaces. The largest default benchmark configuration (Python Notebook with 64 vCores) consumes 32 CUs per second. See [Capacity Requirements](#capacity-requirements) for details. You can run on smaller capacities by reducing the benchmark configurations — see [Scaling Back the Benchmarks](#scaling-back-the-benchmarks)
3. **Fabric Workspace Admin** — permissions to create and configure workspaces

## Repository Structure

```
fabric-performance-benchmark/
├── fabric/
│   ├── benchmark_1_spark_versus_python/
│   │   ├── analysis_of_results/
│   │   │   ├── analysis_of_results.Notebook    # Process raw benchmark data
│   │   │   ├── benchmark_analytics.Report      # Power BI report
│   │   │   └── *.SemanticModel                 # Power BI semantic model
│   │   ├── benchmark_notebooks/
│   │   │   ├── benchmark_1_variables.VariableLibrary  # Pipeline & notebook configuration
│   │   │   ├── duckdb_benchmark.Notebook       # DuckDB implementation
│   │   │   ├── helper_methods.Notebook         # Shared helpers for Python benchmarks
│   │   │   ├── helper_methods_pyspark.Notebook # Shared helpers for Spark benchmarks
│   │   │   ├── pandas_benchmark.Notebook       # Pandas implementation
│   │   │   ├── polars_benchmark.Notebook       # Polars implementation
│   │   │   └── pyspark_benchmark.Notebook      # PySpark implementation
│   │   ├── orchestrate_python_benchmark/
│   │   │   ├── child_pipelines/                # Sub-pipelines for iterations & vCores
│   │   │   └── run_benchmarks.DataPipeline     # Orchestrate Python benchmarks
│   │   ├── orchestrate_spark_benchmark/
│   │   │   ├── child_pipelines/                # Sub-pipelines for iterations & configs
│   │   │   └── run_spark_benchmarks.DataPipeline # Orchestrate Spark benchmarks
│   │   ├── sandpit/                            # Experimental notebooks
│   │   └── set_up/
│   │       ├── configure_workspace.Notebook    # Update variable library with notebook & connection IDs
│   │       └── download_data.Notebook          # Download source data
│   └── fabric_performance_benchmark_lakehouse.Lakehouse
├── notebooks/
│   ├── configure_workspace.ipynb               # Local version of workspace setup
│   ├── fabric-benchmarking-part-1.ipynb        # Local analysis notebook
│   └── fabric-benchmarking-part-1.md           # Blog content
└── src/
    ├── fabric_admin/                           # Fabric REST API client library
    └── onelake_tools/                          # OneLake helper utilities
```

## Getting Started

### 1. Fork the Repository

1. Navigate to [github.com/endjin/fabric-performance-benchmark](https://github.com/endjin/fabric-performance-benchmark)
2. Click **Fork** in the top-right corner
3. Select your GitHub organisation or personal account as the destination
4. Wait for the fork to complete

### 2. Create a Fabric Workspace

1. Sign in to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** in the left navigation pane
3. Click **+ New workspace**
4. Enter the workspace name: `fabric_performance_benchmark_workspace`
   > **Note**: You can choose a different workspace name. The `configure_workspace` notebook will update the `workspace_name` variable in the variable library.
5. Expand **Advanced** and select your Fabric capacity
6. Click **Apply**

### 3. Connect Workspace to GitHub

1. In your new workspace, click **Workspace settings** (gear icon)
2. Navigate to **Git integration**
3. Click **Connect**
4. Select **GitHub** as the Git provider
5. Authenticate with GitHub if prompted
6. Select your forked repository
7. Choose the **main** branch
8. Set the Git folder to `fabric`
9. Click **Connect and sync**

Fabric will import all items from the repository into your workspace. This may take a few minutes.

### 4. Create a Cloud Connection

The benchmark pipelines use a shared cloud connection to trigger notebooks. You must create this connection manually.

1. Go to [Power BI Gateway Management](https://app.powerbi.com/groups/me/gateways)
2. Click **+ New** to create a new connection
3. Set the connection type to **Fabric Data Pipelines**
4. Name it something descriptive (e.g. "Fabric Data Pipelines - Benchmark")
5. Choose the **OAuth 2.0** autentication method
6. Click the **Edit credentials** and complete the authentication wizard
7. Once created, copy the **Connection ID** (GUID) from the connection details — you will need this in the next step

### 5. Configure the Workspace

This step writes the cloud connection GUID and notebook IDs into the variable library so the benchmark pipelines can reference them.

1. Navigate to **set_up** folder in your workspace
2. Open **configure_workspace** notebook
3. Paste the **Connection ID** (GUID) from the previous step into the `CONNECTION_ID` variable
4. Run all cells
5. Verify the output confirms:
   - Variable library updated with the connection GUID and notebook IDs
   - Semantic model repointed to the lakehouse in this workspace

> **Note**: This notebook must be run before the benchmark pipelines. The pipelines depend on the connection GUID and notebook IDs stored in the `benchmark_1_variables` variable library. If you re-sync from Git, you may need to re-run this notebook as Fabric assigns new item IDs.

### 6. Download Source Data

The benchmark uses UK Land Registry house price data (1995-present, ~30 million rows, ~5GB).

1. Navigate to **set_up** folder in your workspace
2. Open **download_data** notebook
3. The notebook defaults to downloading 1 year of data (~100MB). To run the full benchmark as described in our blog post, update `number_of_years` in the `LandRegistryImporter` constructor to `30` for the complete dataset (~5GB, ~30 CSV files)
4. Run all cells to download the CSV files from the Land Registry
5. Verify the data appears in the lakehouse **Files** area under `land_registry/`

> **Note**: Download times depend on the `number_of_years` setting and network speed. The full 30-year dataset may take 10-15 minutes.

### 7. Run the Benchmarks

The benchmarks are orchestrated via Data Factory pipelines that run each engine across multiple configurations. Each pipeline iterates through its configurations **sequentially** (not in parallel), running 3 iterations per configuration to collect statistically meaningful results. Each pipeline takes several hours to complete.

> **Important**: Run the two pipelines **in series** (one after the other), not at the same time, to avoid exceeding your Fabric capacity limits.

#### Run Spark Benchmarks

1. Navigate to **orchestrate_spark_benchmark** folder
2. Open **run_spark_benchmarks** pipeline
3. Review the default parameters:
   - `configurations_to_run` — array of 6 Spark configurations (see [Spark Notebook Configurations](#spark-notebook-configurations))
   - `iterations` — number of runs per configuration (default: `3`)
4. Click **Run** and wait for the pipeline to complete before starting the Python benchmarks
5. Monitor progress in the pipeline run view or the Monitoring hub

The Spark pipeline tests PySpark across various executor configurations (1-4 executors, with 4/4 and 8/8 vCore/memory combinations). The largest configuration (8/8 vCores, 4 executors) consumes 20 CUs per second.

#### Run Python Benchmarks

1. Navigate to **orchestrate_python_benchmark** folder
2. Open **run_benchmarks** pipeline
3. Review the default parameters:
   - `vcores_to_run` — array of vCore sizes (default: `[2, 4, 8, 16, 32, 64]`)
   - `iterations` — number of runs per configuration (default: `3`)
4. Click **Run**
5. Monitor progress in the pipeline run view or the Monitoring hub

The Python pipeline tests Polars, DuckDB, and Pandas across each vCore configuration. The largest configuration (64 vCores) consumes 32 CUs per second.

> **Note**: Each pipeline takes several hours to run due to sequential execution. Spark benchmarks include cluster spin-up time (~3 minutes per run). Python Notebook benchmarks are faster to provision (~30 seconds for the default 2 vCore configuration).

### 9. Analyse Results

After the benchmarks complete, process the raw data:

1. Navigate to **analysis_of_results** folder
2. Open **analysis_of_results** notebook
3. Run all cells to:
   - Aggregate benchmark timing data
   - Calculate median execution times
   - Compute CU costs
   - Generate visualisations

The notebook writes processed data to the `benchmark_repository/benchmark_analytics` Delta table.

### 10. View the Report

1. Navigate to **analysis_of_results** folder
2. Click the **Benchmark Analytics** semantic model (the name may appear as a GUID in the file explorer — look for the item with type **Semantic Model**)
3. Open **benchmark_analytics** report
4. Explore the interactive visualisations:
   - Execution time comparisons
   - Cost analysis
   - Stage-level breakdowns
   - Configuration comparisons

## Local Development

You can run the analysis notebook locally on your machine, which provides access to modern IDEs, faster iteration, and the ability to customise the analysis. The local notebook `notebooks/fabric-benchmarking-part-1.ipynb` connects directly to Fabric and generates detailed analytics with Markdown commentary — this notebook formed the basis of our [blog series on this topic](https://endjin.com/blog/2026/03/fabric-performance-benchmarking-part-1).

### Prerequisites

- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or compatible container runtime)
- Access to a Fabric workspace with benchmark data

### Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/endjin/fabric-performance-benchmark.git
   ```

2. Open the folder in VS Code:

   ```bash
   code fabric-performance-benchmark
   ```

3. When prompted, click **Reopen in Container** (or use the Command Palette: `Dev Containers: Reopen in Container`)

4. Wait for the container to build — this automatically installs Python 3.12 and all dependencies via `uv`

### Run the Local Analysis Notebook

1. Navigate to `notebooks/fabric-benchmarking-part-1.ipynb`
2. Update the workspace and lakehouse names if different from defaults:

   ```python
   WORKSPACE_NAME = "fabric_performance_benchmark_workspace"
   LAKEHOUSE_NAME = "fabric_performance_benchmark_lakehouse"
   ```

3. Run all cells to:
   - Authenticate with OneLake via browser (uses `InteractiveBrowserCredential`)
   - Load benchmark data from the Fabric Lakehouse
   - Generate interactive Plotly visualisations
   - View detailed Markdown commentary explaining each analysis

> **Note**: The local notebook reads data directly from OneLake, so you must have run the benchmarks in Fabric first to have data available for analysis.

## Benchmark Configurations

The benchmark pipelines test each engine across multiple compute configurations. These are defined as pipeline parameters and can be customised before each run.

### Python Notebook Configurations

Configured via the `vcores_to_run` parameter on the **run_benchmarks** pipeline. Each Python Notebook runs on a single compute node. Fabric allocates memory proportionally to the vCore count.

| CUs Per Second | vCores | RAM    |
| -------------- | ------ | ------ |
| 1              | 2      | 16 GB  |
| 2              | 4      | 32 GB  |
| 4              | 8      | 64 GB  |
| 8              | 16     | 128 GB |
| 16             | 32     | 256 GB |
| 32             | 64     | 512 GB |

### Spark Notebook Configurations

Configured via the `configurations_to_run` parameter on the **run_spark_benchmarks** pipeline. Each Spark session has a dedicated driver node plus one or more executor nodes. CU consumption is calculated as: **total vCores ÷ 2**, where total vCores = driver_cores + (executor_cores × executor_number).

| CUs Per Second | Executors | vCores (Driver/Executor) | RAM (Driver/Executor) |
| -------------- | --------- | ------------------------ | --------------------- |
| 4              | 1         | 4/4                      | 28G/28G               |
| 6              | 2         | 4/4                      | 28G/28G               |
| 8              | 1         | 8/8                      | 56G/56G               |
| 10             | 4         | 4/4                      | 28G/28G               |
| 12             | 2         | 8/8                      | 56G/56G               |
| 20             | 4         | 8/8                      | 56G/56G               |

### Capacity Requirements

In Fabric, each Capacity Unit (CU) provides 2 Spark vCores. A running notebook consumes CUs for the duration of its session.

The **largest default Python configuration** (64 vCores) consumes **32 CUs per second**, making **F32** the minimum Fabric SKU required to run all benchmarks as configured. The largest Spark configuration (8/8 vCores, 4 executors = 40 total vCores) consumes 20 CUs per second, which fits within F32.

| SKU | Capacity Units | Max Spark vCores | Can run all defaults? |
| --- | -------------- | ---------------- | --------------------- |
| F2  | 2              | 4                | No                    |
| F4  | 4              | 8                | No                    |
| F8  | 8              | 16               | No                    |
| F16 | 16             | 32               | No                    |
| F32 | 32             | 64               | Yes                   |
| F64 | 64             | 128              | Yes                   |

> **Note**: Fabric applies a 3x burst multiplier for Spark workloads, so the sustained CU consumption matters more than peak vCores. The critical constraint is that the CU consumption of a single benchmark run must not exceed your capacity's CU count.

### Scaling Back the Benchmarks

If your Fabric capacity is smaller than F32, you can reduce the benchmark configurations to fit:

- **Pipeline**: `run_benchmarks` — edit the `vcores_to_run` parameter to remove larger vCore sizes. For example, `[2, 4, 8, 16]` requires only F16
- **Pipeline**: `run_spark_benchmarks` — edit the `configurations_to_run` parameter to remove higher-CU configurations. For example, removing the 4-executor configurations reduces the maximum to 12 CUs (F16)

You can also reduce the `iterations` parameter (default: `3`) to shorten overall run time, though fewer iterations may reduce the statistical reliability of the results.

## Data Attribution

This project uses open data from the [UK Land Registry Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads), made available under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Related Resources

- [DuckDB: The Rise of In-Process Analytics](https://endjin.com/blog/2025/04/duckdb-rise-of-in-process-analytics-understanding-data-singularity)
- [Why Polars Matters for Decision Makers](https://endjin.com/blog/2026/01/why-polars-matters-for-decision-makers)
- [DuckDB Workloads on Microsoft Fabric](https://endjin.com/blog/2025/04/duckdb-workloads-on-microsoft-fabric)
- [Polars Workloads on Microsoft Fabric](https://endjin.com/blog/2026/01/polars-workloads-on-microsoft-fabric)
- [Microsoft Fabric Python Notebooks Documentation](https://learn.microsoft.com/en-us/fabric/data-engineering/using-python-experience-on-notebook)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Troubleshooting

| Problem | Solution |
| ------- | -------- |
| **Pipelines show "item not found" errors** | After syncing from Git, Fabric assigns new IDs. Notebook references are resolved automatically via the Variable Library's Item Reference variables. For child pipeline references, open each pipeline and re-select them from the dropdowns. |
| **Notebooks can't find the lakehouse** | Open each notebook and re-attach the default lakehouse (`fabric_performance_benchmark_lakehouse`) via the lakehouse explorer panel. |
| **Variable library errors** | Ensure the `benchmark_1_variables` variable library exists in the workspace and contains `workspace_name`, `lakehouse_name`, `raw_data_relative_path`, and the notebook Item Reference variables (`polars_benchmark`, `duckdb_benchmark`, `pandas_benchmark`, `pyspark_benchmark`). After syncing from Git, verify the Item Reference variables point to the correct notebook IDs in your workspace. |
| **Spark benchmarks fail with capacity errors** | The full set of default configurations requires F32 capacity. Start with smaller configurations or reduce the `configurations_to_run` and `vcores_to_run` parameters. See [Scaling Back the Benchmarks](#scaling-back-the-benchmarks). |
| **Download notebook fails** | The Land Registry S3 endpoint may be temporarily unavailable. Retry after a few minutes. |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## About endjin

[endjin](https://endjin.com) is a technology consultancy specialising in software engineering, data analytics,  AI and Azure platform.