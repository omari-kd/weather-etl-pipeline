import requests
import pandas as pd
import datetime
from utils import load_config, get_db_connection
from psycopg2.extras import execute_batch

# Cities & coordinates
CITIES = {
    "Accra": {"lat": 5.55, "lon": -0.20},
    "Kumasi": {"lat": 6.69, "lon": -1.63},
    "Takoradi": {"lat": 4.89, "lon": -1.75},
}

# BASE_URL = "https://api.open-meteo.com/v1/forecast"
config = load_config("src/config/config.yaml") 
api_url = config["api_url"]

def fetch_city_weather(city, coords):
    """Fetch hourly temperature for one city."""
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m",
        "forecast_days": 1
    }

    print(f"Fetching weather for {city}...")

    
    response = requests.get(api_url, params=params)
    if response.status_code != 200:
        print(f"API error for {city}: {response.text}")
        return None

    try:
        return response.json()
    except ValueError:
        print(f"Failed to parse JSON for {city}. Response: {response.text[:300]}")
        return None


def transform_to_daily(city, data):
    """Convert hourly records to daily average temperature."""
    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temp": data["hourly"]["temperature_2m"],
    })

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date
    df["city"] = city

    # Daily average
    df_daily = df.groupby("date").agg({
        "temp": "mean",
        "city": "first"
    }).reset_index()

    return df_daily


def insert_daily_weather(conn, df):
    """Insert daily aggregated weather into Postgres."""
    sql = """
        INSERT INTO weather_daily (date, temperature, city)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, city)
        DO UPDATE SET temperature = EXCLUDED.temperature;
    """

    records = df[["date", "temp", "city"]].values.tolist()
    cur = conn.cursor()
    execute_batch(cur, sql, records, page_size=50)
    conn.commit()
    cur.close()


def main():
    print("Loading config...")
    config = load_config("src/config/config.yaml")

    all_daily = []

    # Extract + Transform for each city
    for city, coords in CITIES.items():
        raw = fetch_city_weather(city, coords)
        if raw is None:
            continue
        df_daily = transform_to_daily(city, raw)
        all_daily.append(df_daily)

    if not all_daily:
        print("No valid weather data retrieved.")
        return

    # Combine all cities
    final_df = pd.concat(all_daily, ignore_index=True)
    print("Data ready for loading:")
    print(final_df)

    # Load into Postgres
    print("Connecting to Neon...")
    conn = get_db_connection(config)

    print("Inserting into database...")
    insert_daily_weather(conn, final_df)

    conn.close()

    print("ETL Complete!")


if __name__ == "__main__":
    main()
