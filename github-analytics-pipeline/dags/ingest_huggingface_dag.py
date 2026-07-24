"""Ingest GitHub repository metadata from Hugging Face into a DuckDB warehouse.

This DAG streams a sample of rows from the Hugging Face "codeparrot/github-code"
dataset, transforms the selected fields into a pandas DataFrame, and writes the
result to a DuckDB table under the raw_data schema.
"""

import os
from datetime import datetime
import duckdb
from datasets import load_dataset
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

DB_PATH = "/opt/airflow/warehouse.duckdb"


def extract_data():
    """Extract raw GitHub code metadata and store it in DuckDB.

    The function streams the first 2,000 rows from the Hugging Face
    "codeparrot/github-code" dataset, selects a small set of metadata fields,
    converts the result into a pandas DataFrame, and persists it in the
    raw_data.raw_github_code table inside the configured DuckDB warehouse.
    """
    print("1. Connecting to Hugging Face...")
    data = load_dataset(
        "codeparrot/github-code",
        revision="refs/convert/parquet",
        split="train",
        streaming=True,
    )

    records = []
    for row in data.take(2000):
        records.append(
            {
                "repo_name": row["repo_name"],
                "path": row["path"],
                "language": row["language"],
                "license": row["license"],
                "size": row["size"],
            }
        )

    df = pd.DataFrame(records)
    print(f"2. Extracted {len(df)} rows from Hugging Face.")

    print(f"3. Writing to DuckDB at {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    con.execute(
        "CREATE TABLE IF NOT EXISTS raw_data.raw_github_code AS SELECT * FROM df"
    )

    rows = con.execute("SELECT COUNT(*) FROM raw_data.raw_github_code").fetchone()[0]
    print(f"4. Success! Total rows in warehouse: {rows}")
    con.close()


default_args = {
    "owner": "vmq",
    "retries": 1,
}

with DAG(
    dag_id="01_github_huggingface_ingest",
    default_args=default_args,
    description="Stream GitHub metadata from Hugging Face into DuckDB warehouse",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="extract_hf_data",
        python_callable=extract_data,
    )
