# Dataset Comparator

This repository provides the `DatasetComparator` class, designed for comparing two PySpark DataFrames. The comparator helps ensure data consistency during the migration from an on-premises data warehouse to a cloud-based environment by performing checks for schema differences, row count discrepancies, and value mismatches.


## Features

- **Schema comparison:** Identify differences in the number and types of columns.
- **Row count comparison:** Compare the number of rows between datasets.
- **Value comparison:** Detect rows present in one dataset but missing in the other.
- **Flexible input:** Datasets can come from CSV files, database tables, ...


## Project Structure

- **src/** – Contains the `DatasetComparator` source code.
- **test/** – Contains Unit Tests.


## Setup

1. Clone the repository:  
   ```bash
   git clone https://github.com/RzeznAnita/dataset-comparator.git
   cd dataset-comparator
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/Scripts/activate
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```


## Run DatasetComparator
To run the `DatasetComparator`, follow these steps:

1. Open the file `src/run_comparison.py`.
2. The Dataset Comparator requires two PySpark DataFrames:
   - expected_df - the reference dataset to compare against,
   - actual_df - the dataset to be validated.

   They can be initialized in different ways, for example, by reading from CSV, Parquet files, or by querying databases.
   Initialize the datasets, for example:

   ```python
   expected_dataset = spark.read.csv("data/expected.csv", header=True, inferSchema=True)
   actual_dataset = spark.read.csv("data/actual.csv", header=True, inferSchema=True)
   ```

3. Run the pipeline using the command:

    ```bash
    python -m src.run_comparison
    ```

4. The comparator provides the comparison results in log form:
   <img src="images/logs.png" alt="Logs" width="800"/>

    Additionally, the comparator returns the result as a dictionary, which can be further processed - for example, by saving it as a file or using exported dataframes for further analysis


## Run Tests

Unit tests can be run with the following commands:

    ```bash
    source venv/Scripts/activate
    pytest tests/test_comparator.py
    ```