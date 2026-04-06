"""
DAG 01: TaskFlow API
Demonstrates the modern @task decorator pattern in Airflow 2.x.
Pipeline: fetch weather data → transform → save to file.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

CITIES = ["Sao Paulo", "Rio de Janeiro", "Florianopolis", "Curitiba", "Brasilia"]
OUTPUT_DIR = Path("/tmp/weather_data")


@dag(
    dag_id="01_taskflow_api",
    default_args=DEFAULT_ARGS,
    description="TaskFlow API demo: fetch, transform, save weather data",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "taskflow"],
)
def taskflow_api_demo():

    @task()
    def fetch_weather(city: str) -> dict:
        """Fetches current weather for a city using Open-Meteo."""
        # Geocode
        geo = httpx.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city, "count": 1, "language": "pt"},
            timeout=15,
        ).json()
        if not geo.get("results"):
            raise ValueError(f"City not found: {city}")
        lat, lon = geo["results"][0]["latitude"], geo["results"][0]["longitude"]

        # Forecast
        weather = httpx.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
                "timezone": "America/Sao_Paulo",
            },
            timeout=15,
        ).json()

        current = weather["current"]
        return {
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "temperature_celsius": current["temperature_2m"],
            "humidity_percent": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
            "fetched_at": datetime.utcnow().isoformat(),
        }

    @task()
    def transform_weather(raw: dict) -> dict:
        """Adds derived fields to the weather record."""
        temp = raw["temperature_celsius"]
        return {
            **raw,
            "temperature_fahrenheit": round(temp * 9 / 5 + 32, 1),
            "feels_like": "hot" if temp > 30 else ("warm" if temp > 20 else "cold"),
            "high_wind": raw["wind_speed_kmh"] > 40,
        }

    @task()
    def save_to_file(records: list[dict]) -> str:
        """Saves all transformed records to a JSON file."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.utcnow().strftime("%Y-%m-%d_%H")
        output_path = OUTPUT_DIR / f"weather_{date_str}.json"
        output_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
        print(f"Saved {len(records)} records to {output_path}")
        return str(output_path)

    raw_data = fetch_weather.expand(city=CITIES)
    transformed = transform_weather.expand(raw=raw_data)
    save_to_file(transformed)


taskflow_api_demo()
