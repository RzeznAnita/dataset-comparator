from pyspark.sql import SparkSession
from src.comparator import DatasetComparator


def run():
    """Run comparison."""
    spark = (
        SparkSession.builder.master("local[*]").appName("compare-example").getOrCreate()
    )

    # Prepare two datasets, as shown in the example below. You need:
    # - the expected dataset to compare against
    # - the actual dataset to be compared
    # The datasets can also be sourced from CSV files, database tables, or other data sources.
    # Example:
    # expected_dataset = spark.createDataFrame(
    #     [(1, "alice", 10.0), (2, "bob", None)], ["id", "name", "score"]
    # )
    # actual_dataset = spark.createDataFrame(
    #     [(1, "alice", 10.0), (2, "bob", 5.0), (3, "carol", 7.0)],
    #     ["id", "name", "score"],
    # )

    expected_dataset = None
    actual_dataset = None



    comparator = DatasetComparator(expected_df=expected_dataset, actual_df=actual_dataset)
    # The comparator returns a result as a dictionary, which can be further processed.
    # For example, saved to a file or exported as a DataFrame for further analysis.
    result = comparator.run_checks()


if __name__ == "__main__":
    run()
