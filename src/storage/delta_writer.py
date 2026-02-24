"""Reusable Delta Lake read/write operations.

Provides a simple interface for common Delta Lake operations
including append, overwrite, read, and table existence checks.
"""

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DeltaWriter:
    """Utility class for Delta Lake read and write operations.

    Args:
        spark: Active SparkSession instance.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def write_append(self, df: DataFrame, path: str) -> None:
        """Append data to a Delta table, creating it if needed.

        Args:
            df: DataFrame to write.
            path: Delta table path.
        """
        df.write.format("delta").mode("append").save(path)
        logger.info("Appended %d rows to %s", df.count(), path)

    def write_overwrite(self, df: DataFrame, path: str) -> None:
        """Overwrite a Delta table with new data.

        Args:
            df: DataFrame to write.
            path: Delta table path.
        """
        df.write.format("delta").mode("overwrite").save(path)
        logger.info("Overwrote table at %s", path)

    def read_table(self, path: str) -> DataFrame:
        """Read a Delta table into a DataFrame.

        Args:
            path: Delta table path.

        Returns:
            DataFrame with the table contents.
        """
        return self.spark.read.format("delta").load(path)

    def table_exists(self, path: str) -> bool:
        """Check if a Delta table exists at the given path.

        Args:
            path: Delta table path.

        Returns:
            True if a valid Delta table exists at the path.
        """
        try:
            from delta.tables import DeltaTable

            DeltaTable.forPath(self.spark, path)
            return True
        except Exception:
            return False
