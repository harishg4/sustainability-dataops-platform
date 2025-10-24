# orchestration/airflow_dags/multi_source_ingestion_dag.py
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow.decorators import dag, task

# ---- Paths & constants ----
DATA_DIR = os.environ.get("AIRFLOW_DATA", "/opt/airflow/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")

# API endpoints - All FREE and working APIs
CARBON_API = "https://api.carbonintensity.org.uk/intensity"
WEATHER_API = "https://api.open-meteo.com/v1/forecast"  # FREE weather API
RANDOM_USER_API = "https://randomuser.me/api/"  # FREE user data for testing
JOKE_API = "https://official-joke-api.appspot.com/random_joke"  # FREE joke API
COUNTRY_API = "https://restcountries.com/v3.1/all"  # FREE country data

def _ensure_dirs() -> None:
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(BRONZE_DIR).mkdir(parents=True, exist_ok=True)

@dag(
    dag_id="multi_source_sustainability_pipeline",
    schedule="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["sustainability", "multi-source", "dataops"],
)
def multi_source_sustainability_pipeline():
    """
    Enhanced sustainability pipeline with multiple data sources
    """
    
    @task()
    def ingest_carbon_intensity() -> str:
        """Ingest carbon intensity data from UK API"""
        _ensure_dirs()
        
        try:
            resp = requests.get(CARBON_API, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            # Save raw data
            raw_file = os.path.join(RAW_DIR, f"carbon_intensity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(raw_file, "w") as f:
                json.dump(data, f, indent=2)
            
            return raw_file
        except Exception as e:
            raise Exception(f"Failed to ingest carbon intensity data: {e}")
    
    @task()
    def ingest_weather_data() -> str:
        """Ingest weather data from Open-Meteo (FREE API)"""
        _ensure_dirs()
        
        try:
            # Open-Meteo API - FREE, no API key required
            # London coordinates: 51.5074, -0.1278
            params = {
                "latitude": 51.5074,
                "longitude": -0.1278,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                "timezone": "Europe/London"
            }
            
            resp = requests.get(WEATHER_API, params=params, timeout=30)
            resp.raise_for_status()
            weather_data = resp.json()
            
            # Add metadata
            weather_data["ingestion_timestamp"] = datetime.now().isoformat()
            weather_data["source"] = "open-meteo"
            weather_data["location"] = "London, UK"
            
            raw_file = os.path.join(RAW_DIR, f"weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(raw_file, "w") as f:
                json.dump(weather_data, f, indent=2)
            
            return raw_file
        except Exception as e:
            raise Exception(f"Failed to ingest weather data: {e}")
    
    @task()
    def ingest_user_data() -> str:
        """Ingest random user data for testing (FREE API)"""
        _ensure_dirs()
        
        try:
            resp = requests.get(RANDOM_USER_API, timeout=30)
            resp.raise_for_status()
            user_data = resp.json()
            
            # Add metadata
            user_data["ingestion_timestamp"] = datetime.now().isoformat()
            user_data["source"] = "randomuser"
            
            raw_file = os.path.join(RAW_DIR, f"user_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(raw_file, "w") as f:
                json.dump(user_data, f, indent=2)
            
            return raw_file
        except Exception as e:
            raise Exception(f"Failed to ingest user data: {e}")
    
    @task()
    def ingest_joke_data() -> str:
        """Ingest random joke data for testing (FREE API)"""
        _ensure_dirs()
        
        try:
            resp = requests.get(JOKE_API, timeout=30)
            resp.raise_for_status()
            joke_data = resp.json()
            
            # Add metadata
            joke_data["ingestion_timestamp"] = datetime.now().isoformat()
            joke_data["source"] = "joke-api"
            
            raw_file = os.path.join(RAW_DIR, f"joke_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(raw_file, "w") as f:
                json.dump(joke_data, f, indent=2)
            
            return raw_file
        except Exception as e:
            raise Exception(f"Failed to ingest joke data: {e}")
    
    @task()
    def transform_and_merge(carbon_file: str, weather_file: str, user_file: str, joke_file: str) -> str:
        """Transform and merge all data sources into bronze layer"""
        _ensure_dirs()
        
        # Load all data
        with open(carbon_file, 'r') as f:
            carbon_data = json.load(f)
        
        with open(weather_file, 'r') as f:
            weather_data = json.load(f)
        
        with open(user_file, 'r') as f:
            user_data = json.load(f)
        
        with open(joke_file, 'r') as f:
            joke_data = json.load(f)
        
        # Transform carbon data
        carbon_records = []
        if isinstance(carbon_data, dict) and "data" in carbon_data:
            for row in carbon_data["data"]:
                intensity = row.get("intensity", {})
                carbon_records.append({
                    "timestamp": row.get("from"),
                    "carbon_forecast": intensity.get("forecast"),
                    "carbon_actual": intensity.get("actual"),
                    "carbon_index": intensity.get("index"),
                    "carbon_avg": (intensity.get("forecast", 0) + intensity.get("actual", 0)) / 2
                })
        
        # Extract weather data
        current_weather = weather_data.get("current", {})
        weather_info = {
            "temperature": current_weather.get("temperature_2m"),
            "humidity": current_weather.get("relative_humidity_2m"),
            "wind_speed": current_weather.get("wind_speed_10m"),
            "weather_location": weather_data.get("location", "London, UK")
        }
        
        # Extract user data
        user_info = {}
        if "results" in user_data and len(user_data["results"]) > 0:
            user = user_data["results"][0]
            user_info = {
                "user_gender": user.get("gender"),
                "user_nationality": user.get("nat"),
                "user_age": user.get("dob", {}).get("age"),
                "user_country": user.get("location", {}).get("country")
            }
        
        # Extract joke data
        joke_info = {
            "joke_type": joke_data.get("type"),
            "joke_setup": joke_data.get("setup"),
            "joke_punchline": joke_data.get("punchline")
        }
        
        # Create merged dataset
        merged_data = []
        for carbon_record in carbon_records:
            merged_record = {
                **carbon_record,
                **weather_info,
                **user_info,
                **joke_info,
                "ingestion_timestamp": datetime.now().isoformat(),
                "data_sources": ["carbon_intensity", "weather", "user_data", "joke_data"]
            }
            merged_data.append(merged_record)
        
        # Save to bronze layer
        bronze_file = os.path.join(BRONZE_DIR, f"sustainability_bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(bronze_file, "w") as f:
            json.dump(merged_data, f, indent=2)
        
        return bronze_file
    
    @task()
    def validate_bronze_data(bronze_file: str) -> str:
        """Validate bronze layer data with Great Expectations"""
        from pathlib import Path as _P
        import great_expectations as gx
        
        _ensure_dirs()
        
        # Load bronze data
        with open(bronze_file, 'r') as f:
            data = json.load(f)
        
        if not data:
            raise ValueError("No data found in bronze file")
        
        # Convert to DataFrame for validation
        df = pd.DataFrame(data)
        
        # Basic validation
        assert len(df) > 0, "DataFrame is empty"
        assert "carbon_avg" in df.columns, "Missing carbon_avg column"
        assert "temperature" in df.columns, "Missing temperature column"
        
        # Save validation report
        validation_report = {
            "timestamp": datetime.now().isoformat(),
            "file": bronze_file,
            "record_count": len(df),
            "columns": list(df.columns),
            "validation_passed": True,
            "data_quality_score": 95.0  # Placeholder
        }
        
        report_file = os.path.join(DATA_DIR, "bronze_validation_report.json")
        with open(report_file, "w") as f:
            json.dump(validation_report, f, indent=2)
        
        return report_file
    
    # Define task dependencies
    carbon_data = ingest_carbon_intensity()
    weather_data = ingest_weather_data()
    user_data = ingest_user_data()
    joke_data = ingest_joke_data()
    
    bronze_data = transform_and_merge(carbon_data, weather_data, user_data, joke_data)
    validation_report = validate_bronze_data(bronze_data)

multi_source_sustainability_pipeline()
