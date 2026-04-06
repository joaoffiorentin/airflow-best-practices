"""
DAG 03: Sensors and Custom Hooks
Demonstrates HttpSensor to wait for an API to be available,
then uses a custom hook to fetch and process data.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="03_sensors_and_hooks",
    default_args=DEFAULT_ARGS,
    description="HttpSensor + custom hook: wait for API, then fetch data",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["demo", "sensors", "hooks"],
)
def sensors_and_hooks_demo():

    wait_for_api = HttpSensor(
        task_id="wait_for_open_meteo_api",
        http_conn_id="http_default",
        endpoint="https://api.open-meteo.com/v1/forecast?latitude=-23.5&longitude=-46.6&current=temperature_2m",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    @task()
    def fetch_with_custom_hook(city: str = "Sao Paulo") -> dict:
        """Uses the WeatherHook (custom plugin) to fetch data."""
        from plugins.hooks.weather_hook import WeatherHook
        hook = WeatherHook()
        return hook.get_current_weather(city)

    @task()
    def process_weather(data: dict) -> None:
        """Processes and logs the fetched weather data."""
        temp = data.get("temperature_celsius", 0)
        city = data.get("city", "Unknown")
        print(f"Weather report for {city}:")
        print(f"  Temperature: {temp}°C")
        print(f"  Humidity: {data.get('humidity_percent')}%")
        print(f"  Wind: {data.get('wind_speed_kmh')} km/h")

        if temp > 35:
            print("  ALERT: Extreme heat warning!")
        elif temp < 5:
            print("  ALERT: Cold weather warning!")

    weather_data = fetch_with_custom_hook()
    wait_for_api >> weather_data >> process_weather(weather_data)


sensors_and_hooks_demo()
