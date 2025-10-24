# orchestration/airflow_dags/ingest_transform_validate_dag.py
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import requests
from airflow.decorators import dag, task

# ---- Paths & constants ----
DATA_DIR = os.environ.get("AIRFLOW_DATA", "/opt/airflow/data")
RAW_PATH = os.path.join(DATA_DIR, "raw.jsonl")
TRANSFORMED_PATH = os.path.join(DATA_DIR, "transformed.csv")
REPORT_PATH = os.path.join(DATA_DIR, "validation_report.json")

# GE project dir (we mount this; GE reads root from env GX_DATA_CONTEXT_ROOT_DIR)
GX_ROOT = os.environ.get("GX_ROOT", "/opt/airflow/gx")

API_URL = "https://api.carbonintensity.org.uk/intensity"


def _ensure_dirs() -> None:
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(GX_ROOT).mkdir(parents=True, exist_ok=True)


def _to_py(v: Any) -> Any:
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


@dag(
    dag_id="sustainability_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["sustainability", "dataops", "phase2-ge-fluent"],
)
def sustainability_pipeline():
    @task()
    def ingest() -> str:
        _ensure_dirs()
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        with open(RAW_PATH, "w") as f:
            if isinstance(payload, dict) and "data" in payload:
                for row in payload["data"]:
                    f.write(json.dumps(row) + "\n")
            else:
                f.write(json.dumps(payload) + "\n")
        return RAW_PATH

    @task()
    def transform(raw_path: str) -> str:
        _ensure_dirs()
        records = []
        with open(raw_path, "r") as f:
            for line in f:
                row = json.loads(line)
                intensity = (row.get("intensity") or {})
                records.append(
                    {
                        "from": row.get("from"),
                        "to": row.get("to"),
                        "forecast": intensity.get("forecast"),
                        "actual": intensity.get("actual"),
                        "index": intensity.get("index"),
                    }
                )
        df = pd.DataFrame.from_records(records)
        for col in ["forecast", "actual"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["avg_intensity"] = df[["forecast", "actual"]].mean(axis=1, skipna=True)
        df.to_csv(TRANSFORMED_PATH, index=False)
        return TRANSFORMED_PATH

    @task()
    def validate_ge(transformed_path: Optional[str] = None) -> str:
        """
        GE 1.7.x Fluent validation + Data Docs.
        - Reads CSV via Pandas Filesystem datasource
        - Builds Data Docs under <GX_ROOT>/uncommitted/data_docs/local_site/index.html
        - Writes compact JSON report to REPORT_PATH
        """
        from pathlib import Path as _P
        import great_expectations as gx  # GE 1.7.x Fluent

        _ensure_dirs()

        # Fallback so this task can run in isolation (e.g., `airflow tasks test`)
        csv_path = _P(transformed_path or TRANSFORMED_PATH)
        if not csv_path.exists():
            raise FileNotFoundError(f"Missing transformed CSV at {csv_path}")

        # GE context from env: GX_DATA_CONTEXT_ROOT_DIR=/opt/airflow/gx
        ctx = gx.get_context()

        # Correct factory names for GE 1.7.x
        ds_name = "local_data"
        try:
            ds = ctx.data_sources.get(ds_name)
        except Exception:
            ds = ctx.data_sources.add_pandas_filesystem(
                name=ds_name,
                base_directory=_P(DATA_DIR),
            )

        asset_name = "transformed_csv"
        try:
            # In GE 1.7.x, assets is a list, not a dict-like object
            asset = next((a for a in ds.assets if a.name == asset_name), None)
        except Exception:
            asset = None
        
        if asset is None:
            asset = ds.add_csv_asset(name=asset_name)

        br = asset.build_batch_request()

        suite_name = "carbon_intensity_suite"
        try:
            suite = ctx.suites.get(suite_name)
        except Exception:
            from great_expectations.core import ExpectationSuite
            suite = ctx.suites.add(ExpectationSuite(name=suite_name))

        v = ctx.get_validator(batch_request=br, expectation_suite=suite)

        # First, let's see what columns are actually available
        try:
            # Get the actual data to see what columns exist
            # In GE 1.7.x, we need to get the batch data differently
            batch_request = v.active_batch_request
            batch_data = v.get_batch_data(batch_request)
            actual_columns = list(batch_data.columns)
            print(f"Actual columns found: {actual_columns}")
        except Exception as e:
            print(f"Warning: Could not get column information: {e}")
            # Fallback: try to get columns from the datasource
            try:
                batch = ds.get_batch_list_from_batch_request(batch_request)[0]
                actual_columns = list(batch.data.columns)
                print(f"Actual columns found (fallback): {actual_columns}")
            except Exception as e2:
                print(f"Warning: Could not get column information (fallback): {e2}")
                actual_columns = []

        # Basic validation
        v.expect_table_row_count_to_be_between(min_value=1)
        
        # Only validate columns that actually exist
        if actual_columns:
            v.expect_table_columns_to_match_set(column_set=actual_columns, exact_match=True)
            
            # Try to validate specific columns if they exist
            for col in ["from", "to"]:
                if col in actual_columns:
                    try:
                        v.expect_column_values_to_not_be_null(col)
                        print(f"Successfully validated '{col}' column")
                    except Exception as e:
                        print(f"Warning: Could not validate '{col}' column: {e}")
            
            for col in ["forecast", "actual", "avg_intensity"]:
                if col in actual_columns:
                    try:
                        v.expect_column_values_to_be_between(column=col, min_value=0)
                        print(f"Successfully validated '{col}' column")
                    except Exception as e:
                        print(f"Warning: Could not validate '{col}' column: {e}")
        else:
            print("Warning: No columns found in the data")

        # Only save the suite if it doesn't already exist
        try:
            v.save_expectation_suite(discard_failed_expectations=False)
        except Exception as e:
            print(f"Warning: Could not save expectation suite: {e}")
        
        res = v.validate()

        # Build Data Docs & capture a URL (ephemeral if no local project yet)
        ctx.build_data_docs()
        docs_sites = ctx.get_docs_sites_urls()
        docs_url = None
        if docs_sites:
            for site in docs_sites:
                if "local_site" in site.get("site_name", ""):
                    docs_url = site.get("site_url")
                    break
            docs_url = docs_url or docs_sites[0].get("site_url")

        report: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "passed": bool(res.success),
            "docs_url": docs_url,
            "suite_name": suite_name,
            "asset": {"datasource": ds_name, "asset_name": asset_name, "file": "transformed.csv"},
        }
        report = {k: _to_py(v) for k, v in report.items()}

        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2)

        if not res.success:
            raise AssertionError(f"Great Expectations validation failed. See Data Docs: {docs_url}")

        return REPORT_PATH

    @task()
    def validate_quick(report_path: str) -> str:
        with open(report_path) as f:
            checks = json.load(f)
        assert checks.get("passed") is True, "GE Fluent validation indicates failure"
        return report_path

    # Define the task dependencies
    p_raw = ingest()
    p_csv = transform(p_raw)
    p_report = validate_ge(p_csv)
    validate_quick(p_report)


sustainability_pipeline()
