"""
Custom Airflow Hook for Open-Meteo Weather API.
Demonstrates how to encapsulate external API logic in a reusable hook.
"""
import httpx
from airflow.hooks.base import BaseHook


class WeatherHook(BaseHook):
    """
    Custom hook to interact with the Open-Meteo weather API.
    No authentication required.
    """
    conn_name_attr = "weather_conn_id"
    default_conn_name = "weather_default"
    conn_type = "http"
    hook_name = "Open-Meteo Weather"

    GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, timeout: int = 30):
        super().__init__()
        self.timeout = timeout

    def _geocode(self, city: str) -> tuple[float, float]:
        response = httpx.get(
            self.GEOCODING_URL,
            params={"name": city, "count": 1, "language": "pt"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            raise ValueError(f"City not found: {city}")
        return results[0]["latitude"], results[0]["longitude"]

    def get_current_weather(self, city: str) -> dict:
        """Fetches current weather conditions for a given city."""
        lat, lon = self._geocode(city)
        response = httpx.get(
            self.FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"],
                "timezone": "America/Sao_Paulo",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        current = data["current"]

        return {
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "temperature_celsius": current["temperature_2m"],
            "humidity_percent": current.get("relative_humidity_2m"),
            "wind_speed_kmh": current.get("wind_speed_10m"),
            "precipitation_mm": current.get("precipitation"),
        }
