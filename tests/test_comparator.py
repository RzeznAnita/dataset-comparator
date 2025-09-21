import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, DataFrame
from src.comparator import DatasetComparator


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder.master("local[*]").appName("test-dataset-comparator").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_datasets(spark):
    """Provide sample datasets for testing."""
    expected_df = spark.createDataFrame(
        [(1, "alice", 10.0), (2, "bob", None)],
        ["id", "name", "score"]
    )
    actual_df = spark.createDataFrame(
        [(1, "alice", 10.0), (2, "bob", 5.0), (3, "carol", 7.0)], 
        ["id", "name", "score"]
    )
    return expected_df, actual_df


def test_init_with_valid_dfs(sample_datasets):
    """Test __init__()."""
    expected_df, actual_df = sample_datasets
    comparator = DatasetComparator(expected_df, actual_df)
    assert isinstance(comparator.expected_df, DataFrame)
    assert isinstance(comparator.actual_df, DataFrame)


def test_init_with_invalid_inputs(sample_datasets):
    """Test __init__()."""
    expected_df, actual_df = sample_datasets
    with pytest.raises(TypeError, match="The expected_df is not provided as a PySpark DataFrame"):
        DatasetComparator("Incorrect_DataFrame", actual_df)

    with pytest.raises(TypeError, match="The actual_df is not provided as a PySpark DataFrame"):
        DatasetComparator(expected_df, "Incorrect_DataFrame")


def test_compare_schemas(sample_datasets):
    """Test compare_schemas()."""
    expected_df, actual_df = sample_datasets
    comparator = DatasetComparator(expected_df, actual_df)
    result = comparator.compare_schemas()

    assert isinstance(result, dict)
    assert result["Columns available only in the expected dataset"] is None
    assert result["Columns available only in the actual dataset"] is None
    assert result["Columns that have different data types between the datasets"] is None


def test_compare_row_counts(sample_datasets):
    """Test compare_row_counts()."""
    expected_df, actual_df = sample_datasets
    comparator = DatasetComparator(expected_df, actual_df)
    result = comparator.compare_row_counts()

    assert isinstance(result, dict)
    assert result["Number of rows in the expected dataset"] == 2
    assert result["Number of rows in the actual dataset"] == 3
    assert result["Difference in the number of rows between datasets"] == 1
    assert result["Percentage change in the number of rows between datasets"] == "50.0 %"


def test_compare_values_in_columns(sample_datasets):
    """Test compare_values_in_columns()."""
    expected_df, actual_df = sample_datasets
    comparator = DatasetComparator(expected_df, actual_df)
    result = comparator.compare_values_in_columns()

    assert isinstance(result, dict)
    assert result["Number of rows missing in the expected dataset (available only in the actual dataset) based on common columns"] == 2
    assert result["Number of rows missing in the actual dataset (available only in the expected dataset) based on common columns"] == 1


def test_run_checks(sample_datasets):
    """Test test_run_checks()."""
    expected_df, actual_df = sample_datasets
    comparator = DatasetComparator(expected_df, actual_df)
    comparator.compare_schemas = MagicMock()
    comparator.compare_row_counts = MagicMock()
    comparator.compare_values_in_columns = MagicMock()
    comparator.report_results = MagicMock()

    results = comparator.run_checks()

    assert isinstance(results, dict)
    assert "schema" in results
    assert "row_counts" in results
    assert "value_mismatches" in results

    comparator.compare_schemas.assert_called_once()
    comparator.compare_row_counts.assert_called_once()
    comparator.compare_values_in_columns.assert_called_once()
    assert comparator.report_results.call_count == 3
