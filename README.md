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
2. **Microsoft Fabric Capacity** — an active Fabric capacity (F2 or above recommended) with permissions to create workspaces
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
│   │   │   ├── duckdb_benchmark.Notebook       # DuckDB implementation
│   │   │   ├── pandas_benchmark.Notebook       # Pandas implementation
│   │   │   ├── polars_benchmark.Notebook       # Polars implementation
│   │   │   └── pyspark_benchmark.Notebook      # PySpark implementation
│   │   ├── orchestrate_python_benchmark/
│   │   │   └── run_benchmarks.DataPipeline     # Orchestrate Python benchmarks
│   │   ├── orchestrate_spark_benchmark/
│   │   │   └── run_spark_benchmarks.DataPipeline # Orchestrate Spark benchmarks
│   │   └── set_up/
│   │       └── download_data.Notebook          # Download source data
│   └── fabric_performance_benchmark_lakehouse.Lakehouse
├── notebooks/
│   ├── analysis_of_results.ipynb               # Local analysis notebook
│   └── fabric-benchmarking-part-1.md           # Blog content
└── src/
    └── onelake_tools/                          # Helper utilities
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
   > **Note**: The default name matches the configuration in the benchmark notebooks. If you use a different name, you'll need to update the `workspace_name` variable in the variable library.
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
8. Set the Git folder to `/fabric`
9. Click **Connect and sync**

Fabric will import all items from the repository into your workspace. This may take a few minutes.

### 4. Configure the Lakehouse

After syncing, verify the lakehouse is correctly configured:

1. Open **fabric_performance_benchmark_lakehouse** in your workspace
2. The lakehouse should be empty initially — this is expected
3. The benchmark notebooks will write data to this lakehouse

### 5. Download Source Data

The benchmark uses UK Land Registry house price data (1995-present, ~30 million rows, ~5GB).

1. Navigate to **set_up** folder in your workspace
2. Open **download_data** notebook
3. Run all cells to download the CSV files from the Land Registry
4. Verify the data appears in the lakehouse **Files** area under `land_registry/`

> **Note**: The download may take 10-15 minutes depending on network speed. The notebook downloads approximately 30 CSV files (one per year).

### 6. Run the Benchmarks

The benchmarks are orchestrated via Data Factory pipelines that run each engine across multiple configurations. Each pipeline runs the benchmark notebooks multiple times to collect statistically meaningful results.

#### Run Spark Benchmarks

1. Navigate to **orchestrate_spark_benchmark** folder
2. Open **run_spark_benchmarks** pipeline
3. Click **Run**
4. Monitor progress in the pipeline run view

The Spark pipeline tests PySpark across various executor configurations (1-4 executors, different vCore/memory combinations).

#### Run Python Benchmarks

1. Navigate to **orchestrate_python_benchmark** folder
2. Open **run_benchmarks** pipeline
3. Click **Run**
4. Monitor progress in the pipeline run view

The Python pipeline tests Pandas, Polars, and DuckDB across various Python Notebook configurations (2-32 vCores).

> **Note**: Running all benchmarks takes several hours. Spark benchmarks include cluster spin-up time (~3 minutes per run). Python Notebook benchmarks are faster to provision (~30 seconds for default configuration).

### 7. Analyse Results

After the benchmarks complete, process the raw data:

1. Navigate to **analysis_of_results** folder
2. Open **analysis_of_results** notebook
3. Run all cells to:
   - Aggregate benchmark timing data
   - Calculate median execution times
   - Compute CU costs
   - Generate visualisations

The notebook writes processed data to the `benchmark_repository/benchmark_analytics` Delta table.

### 8. View the Report

1. Navigate to **analysis_of_results** folder
2. Click **c95114c9-5174-ad7a-4ae1-e3a2a8f7a9ab** semantic model
3. Click **Refresh now** to load the latest benchmark data
4. Open **benchmark_analytics** report
5. Explore the interactive visualisations:
   - Execution time comparisons
   - Cost analysis
   - Stage-level breakdowns
   - Configuration comparisons

## Benchmark Configurations

### Python Notebook Configurations

| CUs Per Second | vCores | RAM    |
| -------------- | ------ | ------ |
| 1              | 2      | 16 GB  |
| 2              | 4      | 32 GB  |
| 4              | 8      | 64 GB  |
| 8              | 16     | 128 GB |
| 16             | 32     | 256 GB |

### Spark Notebook Configurations

| CUs Per Second | Executors | vCores (Driver/Executor) | RAM (Driver/Executor) |
| -------------- | --------- | ------------------------ | --------------------- |
| 4              | 1         | 4/4                      | 28G/28G               |
| 8              | 1         | 8/8                      | 56G/56G               |
| 12             | 2         | 8/8                      | 56G/56G               |
| 20             | 4         | 8/8                      | 56G/56G               |

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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## About endjin

[endjin](https://endjin.com) is a technology consultancy specialising in Data, AI & Advanced Analytics, and Azure Platform Engineering. We help organisations make better decisions through data.
