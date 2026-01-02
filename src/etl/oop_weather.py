"""
Weather ETL Pipeline using Object-Oriented Programming.

This module implements an ETL (Extract, Transform, Load) pipeline for weather data.
It fetches weather data from an API, transforms it into daily summaries, and loads it into a PostgreSQL database.
The pipeline is structured using classes for separation of concerns: API client, transformer, repository, and the main pipeline orchestrator.
"""

import requests
import pandas as pd
from psycopg2.extras import execute_batch
from utils import load_config, get_db_connection

# Dictionary of cities with their latitude and longitude coordinates
CITIES = {
    "Accra": {
        "lat": 5.55,
        "lon": -0.20,
    },
    "Kumasi": {
        "lat": 6.69,
        "lon": -1.63
    },
    "Takoradi": {
        "lat": 4.89,
        "lon": -1.75
    }
}

class WeatherAPIClient:
    """
    Client for interacting with the weather API.

    This class handles fetching weather data from an external API for given cities.
    """

    def __init__(self, api_url: str):
        """
        Initialize the API client with the base URL.

        Args:
            api_url (str): The base URL of the weather API.
        """
        self.api_url = api_url

    def fetch_city_weather(self, city: str, coords: dict) -> dict | None:
        """
        Fetch hourly weather data for a specific city.

        Args:
            city (str): Name of the city.
            coords (dict): Dictionary with 'lat' and 'lon' keys for coordinates.

        Returns:
            dict or None: JSON response from the API if successful, None otherwise.
        """
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": "temperature_2m",
            "forecast_days": 1  # Fixed typo: was 'forecasr_days'
        }

        print(f"Fetching weather for {city}...")

        response = requests.get(self.api_url, params=params)

        if response.status_code != 200:
            print(f"API error for {city}: {response.text}")
            return None

        try:
            return response.json()
        except ValueError:
            print(f"Failed to parse JSON for {city}")
            return None
        
class WeatherTransformer:
    """
    Handles transformation of weather data.

    This class contains methods to process raw weather data into usable formats.
    """

    @staticmethod
    def hourly_to_daily(city: str, data: dict) -> pd.DataFrame:
        """
        Transform hourly weather data into daily summaries.

        Args:
            city (str): Name of the city.
            data (dict): Raw weather data from the API.

        Returns:
            pd.DataFrame: DataFrame with daily average temperatures.
        """
        df = pd.DataFrame({
            "datetime": data["hourly"]["time"],
            "temp": data["hourly"]["temperature_2m"]
        })

        df["datetime"] = pd.to_datetime(df["datetime"])
        df["date"] = df["datetime"].dt.date
        df["city"] = city

        df_daily = (
            df.groupby("date")
            .agg(temp=("temp", "mean"), city=("city", "first"))
            .reset_index()
        )

        return df_daily
    

class WeatherRepository:
    """
    Repository for database operations related to weather data.

    This class handles inserting weather data into the database.
    """

    def __init__(self, conn):
        """
        Initialize the repository with a database connection.

        Args:
            conn: Database connection object.
        """
        self.conn = conn

    def insert_daily_weather(self, df):
        """
        Insert daily weather data into the database.

        Uses batch insert with conflict resolution to update existing records.

        Args:
            df (pd.DataFrame): DataFrame with daily weather data.
        """
        sql = """
            INSERT INTO weather_daily (date, temperature, city)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, city)
            DO UPDATE SET temperature = EXCLUDED.temperature;
        """

        records = df[["date", "temp", "city"]].values.tolist()

        with self.conn.cursor() as cur:
            execute_batch(cur, sql, records, page_size=50)

        self.conn.commit()


class WeatherETLPipeline:
    """
    Main ETL pipeline orchestrator.

    This class coordinates the extraction, transformation, and loading of weather data.
    """

    def __init__(self, config_path: str, cities: dict):
        """
        Initialize the pipeline with configuration and cities.

        Args:
            config_path (str): Path to the configuration YAML file.
            cities (dict): Dictionary of cities with coordinates.
        """
        self.config = load_config(config_path)
        self.cities = cities

        self.apiclient = WeatherAPIClient(self.config["api_url"])
        self.transformer = WeatherTransformer()

    def run(self):
        """
        Execute the ETL pipeline.

        Extracts weather data for all cities, transforms it to daily summaries,
        and loads it into the database.
        """
        all_daily = []

        # Extract + Transform
        for city, coords in self.cities.items():
            raw = self.apiclient.fetch_city_weather(city, coords)
            if raw is None:
                continue

            df_daily = self.transformer.hourly_to_daily(city, raw)
            all_daily.append(df_daily)

        if not all_daily:
            print("No valid weather data retrieved")
            return

        final_df = pd.concat(all_daily, ignore_index=True)
        print("Data ready for loading:")
        print(final_df)

        # Load
        conn = get_db_connection(self.config)  # Fixed: was get_db_connection(self, config)
        repo = WeatherRepository(conn)

        print("Inserting into database...")
        repo.insert_daily_weather(final_df)

        conn.close()
        print("ETL Complete!")



if __name__ == "__main__":
    # Create and run the ETL pipeline when the script is executed directly
    pipeline = WeatherETLPipeline(
        config_path="src/config/config.yaml",
        cities=CITIES
    )
    pipeline.run()
