"""Tests for Delta Lake write operations."""

from pyspark.sql import SparkSession

from src.storage.delta_writer import DeltaWriter


class TestDeltaWriter:
    """Tests for the DeltaWriter utility class."""

    def test_write_append_creates_table(self, spark: SparkSession, tmp_path) -> None:
        writer = DeltaWriter(spark)
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        path = str(tmp_path / "test_table")
        writer.write_append(df, path)
        result = writer.read_table(path)
        assert result.count() == 2

    def test_write_append_adds_rows(self, spark: SparkSession, tmp_path) -> None:
        writer = DeltaWriter(spark)
        path = str(tmp_path / "append_table")
        df1 = spark.createDataFrame([(1, "a")], ["id", "value"])
        df2 = spark.createDataFrame([(2, "b")], ["id", "value"])
        writer.write_append(df1, path)
        writer.write_append(df2, path)
        result = writer.read_table(path)
        assert result.count() == 2

    def test_write_overwrite_replaces_data(self, spark: SparkSession, tmp_path) -> None:
        writer = DeltaWriter(spark)
        path = str(tmp_path / "overwrite_table")
        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        df2 = spark.createDataFrame([(3, "c")], ["id", "value"])
        writer.write_append(df1, path)
        writer.write_overwrite(df2, path)
        result = writer.read_table(path)
        assert result.count() == 1
        assert result.collect()[0]["id"] == 3

    def test_table_exists_true(self, spark: SparkSession, tmp_path) -> None:
        writer = DeltaWriter(spark)
        path = str(tmp_path / "exists_table")
        df = spark.createDataFrame([(1,)], ["id"])
        writer.write_append(df, path)
        assert writer.table_exists(path) is True

    def test_table_exists_false(self, spark: SparkSession, tmp_path) -> None:
        writer = DeltaWriter(spark)
        path = str(tmp_path / "nonexistent_table")
        assert writer.table_exists(path) is False

    def test_read_table_returns_correct_schema(
        self, spark: SparkSession, tmp_path
    ) -> None:
        writer = DeltaWriter(spark)
        path = str(tmp_path / "schema_table")
        df = spark.createDataFrame([(1, "a", 3.14)], ["id", "name", "value"])
        writer.write_append(df, path)
        result = writer.read_table(path)
        col_names = [f.name for f in result.schema.fields]
        assert "id" in col_names
        assert "name" in col_names
        assert "value" in col_names
