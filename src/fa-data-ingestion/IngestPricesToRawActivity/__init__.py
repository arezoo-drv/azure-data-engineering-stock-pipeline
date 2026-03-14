

import logging
import json
import os
import requests
import pyodbc
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
#_____________________________________________
def fetch_daily_prices(symbol: str, api_key: str):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "outputsize": 5,#outputsize,
        "apikey": api_key
    }

    resp = requests.get(url, params=params, timeout=30)

    return resp
#__________________________________________
def main(name: dict) -> dict:
    symbols = name["symbols"]
    source_name = name["source_name"]
    run_id = name["run_id"]

    api_key = os.environ["TWELVEDATA_API_KEY"]
    #outputsize = int(os.environ.get("OUTPUTSIZE_DAYS", "5"))

    conn = get_sql_connection()
    cursor = conn.cursor()

    total_inserted = 0
    failed_symbols = []

    for symbol in symbols:
        api_call_id = None
        requested_at_utc = datetime.utcnow()

        try:
            # 1) Insert api_call_log (start)
            cursor.execute(
                """
                INSERT INTO raw.api_call_log
                    (run_id,source_name,  endpoint_name, symbol,resolution, requested_at_utc, is_success)
                OUTPUT INSERTED.api_call_id
                VALUES (?, ?,?, ?, ?,?, 0);
                
                """,
                run_id,source_name,  "twelvedata_time_series", symbol,"1day", requested_at_utc
            ) 
            api_call_id = cursor.fetchone()[0]
            conn.commit()
           
        except:
                conn.rollback()
        try:
            # 2) Call API
            resp = fetch_daily_prices(symbol, api_key)#, outputsize=outputsize
            http_status = resp.status_code
            resp.raise_for_status()
            data = resp.json()

            
            if "values" not in data:
                raise ValueError(f"No values in response: {data}")

            rows = data["values"]

            # 3) Insert into raw (api_call_id FK)
            for r in rows:
                cursor.execute(
                    """
                    INSERT INTO raw.stock_candle_daily
                    (symbol, candle_date, open_price, high_price, low_price, close_price, volume, source_name , api_call_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ? , ?)
                    """,
                    symbol,
                    datetime.strptime(r["datetime"], "%Y-%m-%d").date(),
                    float(r["open"]),
                    float(r["high"]),
                    float(r["low"]),
                    float(r["close"]),
                    int(r["volume"]) if r.get("volume") else None,
                    source_name,
                    api_call_id
                )
                total_inserted += 1

 
             # 4) Update api_call_log success
            cursor.execute(
                """
                UPDATE raw.api_call_log
                   SET is_success = 1,
                       http_status = ?,
                       response_json = ?,
                       error_message = NULL
                 WHERE api_call_id = ?;
                """,
                http_status,
                json.dumps({"meta": data.get("meta"), "count": len(rows)}),
                api_call_id
            )

            conn.commit()

        except Exception as e:
            err_detail = str(e)
            failed_symbols.append(f"{symbol}: {str(e)}")
             # rollback for this symbol
            conn.rollback()

            # best-effort update api_call_log failure (if we have api_call_id)
            try:
                if api_call_id is not None:
                    cursor.execute(
                        """
                        UPDATE raw.api_call_log
                           SET is_success = 0,
                               error_message = ?
                         WHERE api_call_id = ?;
                        """,
                        str(e),
                        api_call_id
                    )
                    conn.commit()
            except:
                conn.rollback()
        

    cursor.close()
    conn.close()

    return {
        "status": "Partial" if failed_symbols else "Success",
        "rows_inserted_raw": total_inserted,
        "error_message": "; ".join(failed_symbols) if failed_symbols else None
    }
