# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Callable, Any
from urllib.parse import quote
import psutil
import notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Helper function to create base ABFSS path based on workspace and lakehouse name
def construct_base_abfss_path(workspace_name: str, lakehouse_name: str) -> str:
    """Construct the base ABFSS path for a given workspace and lakehouse."""
    workspace_name = quote(workspace_name, safe="")
    lakehouse_name = quote(lakehouse_name, safe="")
    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse"


# Helper function to create storage options that enable data tools to authenticate and interact with onelake storage
def create_storage_options() -> dict:
    return {
        "bearer_token": notebookutils.credentials.getToken("storage"),
        "use_fabric_endpoint": "true",
    }


# Data class to store benchmark metrics at key points during notebook process
@dataclass
class Benchmark:
    platform: str
    configuration: str
    workload_name: str
    run_timestamp: str
    stage_name: str
    stage_time: float
    cpu_count: float
    cpu_usage: float
    memory: float
    memory_usage: float


# Benchmark Manager class will help capture benchmarks consistently, then write them out to lakehouse at end of notebook
class BenchmarkManager:

    def __init__(
        self,
        platform: str,
        configuration: str,
        workload_name: str,
        run_timestamp: str,
        export_abfss_path: str,
        storage_options: dict,
        exporter: Callable[["BenchmarkManager"], Any],
    ):
        self.platform = platform
        self.configuration = configuration
        self.workload_name = workload_name
        self.run_timestamp = run_timestamp
        self.export_abfss_path = export_abfss_path
        self.storage_options = storage_options
        self.benchmarks = []
        self._exporter = exporter

    def capture_benchmark(self, stage_name, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()

        self.benchmarks.append(
            Benchmark(
                platform=self.platform,
                configuration=self.configuration,
                workload_name=self.workload_name,
                run_timestamp=self.run_timestamp,
                stage_name=stage_name,
                stage_time=timestamp,
                cpu_count=psutil.cpu_count(),
                cpu_usage=psutil.cpu_percent(interval=None),
                memory=(psutil.virtual_memory().total / (1024 * 1024 * 1024)),
                memory_usage=psutil.virtual_memory().percent,
            )
        )

    def export_results(self):
        return self._exporter(self)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
