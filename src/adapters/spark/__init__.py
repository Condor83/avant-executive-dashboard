"""Spark adapter package."""

from adapters.spark.adapter import EvmRpcSparkClient, SparkAdapter

__all__ = [
    "SparkAdapter",
    "EvmRpcSparkClient",
]
