# Vessel Identification Pipeline

## Overview

This project is an end-to-end data engineering pipeline that processes raw, noisy vessel data from a CSV file to produce a clean and comprehensive set of "golden records". It identifies unique vessels and consolidates their attributes, creating a single source of truth that is stored in a PostgreSQL database.

The pipeline is fully containerized using Docker and Docker Compose, making it portable and easy to run.

---

## Architecture

The application runs as a multi-container setup managed by Docker Compose, consisting of two main services:

1. `postgres-db`: A PostgreSQL database container to store the final, cleaned data.
2. `spark-app`: A container that runs a PySpark script to perform the core data processing.

```
[source CSV] -> spark-app (PySpark) -> postgres-db (golden_records table)
```

---

## Prerequisites

* Docker
* Docker Compose

---

## How to Run

1. **Build and Run the Pipeline**

    From the root of the project directory, run the following command:

    ```bash
    docker-compose up --build
    ```

    This command will:
    * Build the Docker image for the `spark-app` service.
    * Start the `postgres-db` container.
    * Once the database is ready, it will start the `spark-app` container.
    * The Spark job will automatically run, process the data, and save the results to the `golden_records` table in the database.
    * The process will exit automatically once the Spark job is complete.

2. **Verify the Output**

    Once the job has finished, you can query the PostgreSQL database to see the results. The following command will connect to the database and show the first 10 golden records:

    ```bash
    docker-compose exec postgres-db psql -U sparkuser -d vessels -c "SELECT imo, name_history, builtYear, destination FROM golden_records LIMIT 10;"
    ```

    To observe our previous example which is most frequently occurring IMO (To view the output in a more readable, expanded format for wide tables by using the `-x` flag):

    ```bash
    docker-compose exec postgres-db psql -x -U sparkuser -d vessels -c "SELECT * FROM golden_records WHERE imo = '9710749';"
    ```

---

## Core Logic (`vessel_identification_spark.py`)

The Spark script performs the data processing in several key stages:

1. **Ingestion and Cleaning**
    * The raw CSV data is read into a Spark DataFrame.
    * A custom function cleans the CSV header, creating unique names for columns that appear more than once (e.g., `draught` and `draught_1`).
    * The `imo` column is cast to an integer type to ensure data quality.
    * Rows with invalid IMO numbers are filtered out using a checksum validation algorithm.

2. **Golden Record Aggregation**

    To create a single, comprehensive record for each vessel (`imo`), a three-part aggregation strategy is used:

    * **Static Attributes (Mode):** For attributes that should be stable (e.g., `builtYear`, `vessel_type`, `length`), the script calculates the most frequently occurring value (**mode**) across all records for that vessel. This makes the result resilient to data entry errors in older records.

    * **Dynamic Attributes (Latest):** For attributes that change with each voyage (e.g., `destination`, `eta`, `last_position_speed`), the script selects the single most recent value by using a Window function partitioned by `imo` and ordered by `UpdateDate`.

    * **Evolving Attributes (History):** For key identifiers that can change over time (e.g., `name`, `flag`, `mmsi`), the script collects a complete set of unique historical values using `collect_set`. This ensures no historical data is lost.

3. **Loading**
    * The three aggregated datasets (static, dynamic, and historical) are joined together on the `imo` key.
    * The historical array columns are converted to clean, comma-separated strings.
    * The final, complete DataFrame is written to the `golden_records` table in the PostgreSQL database using a JDBC connection.
