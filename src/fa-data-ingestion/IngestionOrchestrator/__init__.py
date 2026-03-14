
import logging
import json
import os
import uuid
import datetime as dt
import azure.durable_functions as df
from azure.durable_functions import DurableOrchestrationContext, Orchestrator



def orchestrator_function(context: df.DurableOrchestrationContext):
    run_id = str(uuid.uuid4())

    # Durable-safe timestamps
    run_started_at = context.current_utc_datetime

    symbols_env = os.environ.get("SYMBOLS", "AAPL,RY,TD,SU")
    symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]

    source_name = os.environ.get("SOURCE_NAME", "twelvedata")

    status = "Success"
    error_message = None
    rows_inserted_raw = 0

    try:
        ingest_result = yield context.call_activity(
            "IngestPricesToRawActivity",
            {
                "symbols": symbols,
                "source_name": source_name,
                "run_id": run_id
            }
        )

        status = ingest_result.get("status", "Success")
        rows_inserted_raw = int(ingest_result.get("rows_inserted_raw", 0) or 0)
        error_message = ingest_result.get("error_message")

    except Exception as e:
        status = "Failed"
        error_message = f"{type(e).__name__}: orchestration failed"


    stg_result = yield context.call_activity(
    "LoadStgFromRawActivity",
    {
        
         "since_utc": run_started_at.isoformat()
    }
    )

    if stg_result.get("status") != "Success":
        status = "Partial"
        error_message = (error_message or "") + " | STG: " + (stg_result.get("error_message") or "")




    run_ended_at = context.current_utc_datetime

    # Always write audit log (append-only)
    yield context.call_activity(
        "WriteRunLogActivity",
        {
            "run_id": run_id,
            "orchestration_id": context.instance_id,
            "run_started_at": run_started_at.isoformat(),
            "run_ended_at": run_ended_at.isoformat(),
            "status": status,
            "symbols": ",".join(symbols),
            "rows_inserted_raw": rows_inserted_raw,
            "error_message": error_message
        }
    )

    return {
        "run_id": run_id,
        "status": status,
        "rows_inserted_raw": rows_inserted_raw,
        "symbols": symbols,
        "error_message": error_message
    }


main = df.Orchestrator.create(orchestrator_function)
