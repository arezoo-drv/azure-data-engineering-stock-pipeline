# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import os
import pyodbc
import logging
from datetime import datetime

def get_sql_connection():
    server = os.environ["DB_SERVER"].strip()
    db     = os.environ["DB_NAME"].strip()
    user   = os.environ["DB_USER"].strip()
    pwd    = os.environ["DB_PASSWORD"].strip()

    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={db};"
        f"Uid={user};"
        f"Pwd={pwd};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str)

def main(name: dict) -> dict:
    """
    Input (dict) example:
    {
      "since_utc": "2026-02-14T00:00:00Z"   # optional
    }
    If since_utc omitted, SP uses watermark.
    """
    since_utc = (name or {}).get("since_utc")  # optional ISO string

    conn = None
    cursor = None

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        started_at = datetime.utcnow().isoformat() + "Z"
        logging.info("LoadStgFromRawActivity started_at=%s since_utc=%s", started_at, since_utc)

        # If since_utc is provided, pass it; otherwise pass NULL to use watermark.
        if since_utc:
            cursor.execute("EXEC stg.usp_load_stock_candle_daily @SinceUtc = ?", since_utc)
        else:
            cursor.execute("EXEC stg.usp_load_stock_candle_daily @SinceUtc = NULL")

        # pyodbc rowcount behavior can be -1 for some statements; we still return something helpful
        rows_affected = cursor.rowcount

        conn.commit()

        ended_at = datetime.utcnow().isoformat() + "Z"
        return {
            "status": "Success",
            "rows_affected": rows_affected,
            "started_at_utc": started_at,
            "ended_at_utc": ended_at,
            "error_message": None
        }

    except Exception as e:
        detail = " | ".join(map(str, getattr(e, "args", []) or [e]))
        logging.exception("LoadStgFromRawActivity failed: %s", detail)

        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        return {
            "status": "Failed",
            "rows_affected": 0,
            "error_message": detail
        }

    finally:
        try:
            if cursor:
                cursor.close()
        finally:
            if conn:
                conn.close()
