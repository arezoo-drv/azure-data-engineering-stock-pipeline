# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import os
import pyodbc

def get_sql_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={os.environ['DB_SERVER']};"
        f"DATABASE={os.environ['DB_NAME']};"
        f"UID={os.environ['DB_USER']};"
        f"PWD={os.environ['DB_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)

def main(name: dict) -> None:
    conn = get_sql_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO audit.ingestion_runs
        (run_id, orchestration_id, run_started_at, run_ended_at, status,
         symbols, rows_inserted_raw, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        name["run_id"],
        name["orchestration_id"],
        name["run_started_at"],
        name["run_ended_at"],
        name["status"],
        ",".join(name["symbols"]),
        name["rows_inserted_raw"],
        name["error_message"]
    )

    conn.commit()
    cursor.close()
    conn.close()
