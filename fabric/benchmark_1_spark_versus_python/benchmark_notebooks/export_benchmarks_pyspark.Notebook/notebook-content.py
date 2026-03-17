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

from pyspark.sql import Window
from pyspark.sql import functions as F


def export_with_spark(manager):
    """Export benchmark results using PySpark and write to Delta Lake."""

    records = spark.createDataFrame([asdict(b) for b in manager.benchmarks])

    window_spec = Window.orderBy("stage_time")

    records = (
        records.orderBy("stage_time")
        .withColumn("order", F.row_number().over(window_spec))
        .withColumn(
            "stage_time_delta",
            F.round(
                F.col("stage_time").cast("double")
                - F.lag("stage_time", 1).over(window_spec).cast("double"),
                3,
            ),
        )
        .withColumn("stage_time", F.col("stage_time").cast("timestamp_ntz"))
    )

    records.write.format("delta").mode("append").save(manager.export_abfss_path)
    return records


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
