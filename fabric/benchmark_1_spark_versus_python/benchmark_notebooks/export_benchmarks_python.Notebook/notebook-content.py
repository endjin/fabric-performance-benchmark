# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from dataclasses import asdict

import polars as pl


def export_with_polars(manager):
    """Export benchmark results using Polars and write to Delta Lake."""
    records = pl.DataFrame([asdict(b) for b in manager.benchmarks])
    records = (
        records.sort("stage_time", descending=False)
        .with_row_index("order", offset=1)
        .with_columns(
            (pl.col("stage_time").diff().dt.total_milliseconds().alias("stage_time_delta") / 1000)
        )
    )
    records.write_delta(
        manager.export_abfss_path, mode="append", storage_options=manager.storage_options
    )
    return records


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
