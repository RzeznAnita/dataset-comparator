from functools import reduce

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from src.logger_setup import logger


class DatasetComparator:
    """Dataset comparison class."""
    def __init__(self, expected_df: DataFrame, actual_df: DataFrame):
        """
        Initialize the comparison class with two datasets.

        Params:
            expected_df: The expected dataset to compare against.
            actual_df: The actual dataset to be compared.
        """
        if not isinstance(expected_df, DataFrame):
            raise TypeError("expected_df must be a PySpark DataFrame")

        if not isinstance(actual_df, DataFrame):
            raise TypeError("actual_df must be a PySpark DataFrame")

        self.expected_df = expected_df
        self.actual_df = actual_df

    def compare_schemas(self) -> dict:
        """Dataset schema comparison."""
        schema_expected = {col.name: col.dataType.simpleString() for col in self.expected_df.schema.fields}
        schema_actual = {col.name: col.dataType.simpleString() for col in self.actual_df.schema.fields}

        common_cols = schema_expected.keys() & schema_actual.keys()

        cols_only_in_expected = schema_expected.keys() - schema_actual.keys()
        cols_only_in_actual = schema_actual.keys() - schema_expected.keys()

        type_mismatches = {col: (schema_expected[col], schema_actual[col])
                           for col in common_cols 
                           if schema_expected[col] != schema_actual[col]}

        return {"Columns available only in the expected dataset": cols_only_in_expected or None,
                "Columns available only in the actual dataset": cols_only_in_actual or None,
                "Columns that have different data types between the datasets": type_mismatches or None}

    def compare_row_counts(self) -> dict:
        """Record count comparison."""
        count_expected = self.expected_df.count()
        count_actual = self.actual_df.count()
        diff = abs(count_expected - count_actual)
        pct = round((diff / count_expected) * 100, 2) if count_expected else None

        return {"Number of rows in the expected dataset": count_expected,
                "Number of rows in the actual dataset": count_actual,
                "Difference in the number of rows between datasets": diff,
                "Percentage change in the number of rows between datasets": f"{pct} %"}

    def compare_values_in_columns(self) -> dict:
        """Comparison of values from common columns in two datasets."""
        # outer join on all common columns
        expected_df = self.expected_df.alias("expected_df")
        actual_df = self.actual_df.alias("actual_df")

        common_columns = list(set(expected_df.columns) & set(actual_df.columns))

        joined_dfs = expected_df.join(actual_df, on=common_columns, how="outer")

        # detecting records with discrepancies
        missing_expected_df_expr = reduce(lambda true_cond, col: true_cond & f.col(f"expected_df.{col}").isNull(), common_columns, f.lit(True))
        missing_actual_df_expr = reduce(lambda true_cond, col: true_cond & f.col(f"actual_df.{col}").isNull(), common_columns, f.lit(True))
        missing_expected_df = joined_dfs.filter(missing_expected_df_expr)
        missing_actual_df = joined_dfs.filter(missing_actual_df_expr)

        return {
            "Number of rows missing in the expected dataset (available only in the actual dataset) based on common columns": missing_expected_df.count(),
            "Number of rows missing in the actual dataset (available only in the expected dataset) based on common columns": missing_actual_df.count(),
            "Rows missing in the expected dataset (available only in the actual dataset) based on common columns": missing_expected_df,
            "Rows missing in the actual dataset (available only in the expected dataset) based on common columns": missing_actual_df
        }

    def report_results(self, results: dict) -> None:
        """Report comparison results."""
        for desc, result in results.items():
            if isinstance(result, DataFrame):
                logger.info(f"{desc}:")
                result.show(False, 10)
            else:
                logger.info(f"{desc}: {result}")

    def run_checks(self) -> dict:
        """Run checks."""
        logger.info("Start comparison.")
        # Run checks
        schema_check = self.compare_schemas()
        row_counts_check = self.compare_row_counts()
        values_check = self.compare_values_in_columns()

        # Log results
        self.report_results(schema_check)
        self.report_results(row_counts_check)
        self.report_results(values_check)

        logger.info("Comparison completed.")
        return {
            "schema": schema_check,
            "row_counts": row_counts_check,
            "value_mismatches": values_check
        }
