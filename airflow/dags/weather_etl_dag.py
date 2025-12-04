from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import yaml
import os

# -----------------------------
# CONFIG
# -----------------------------
CITIES = {
    "Accra": {"lat": 5.55, "lon": -0.20},
    "Kumasi": {"lat": 6.69, "lon": -1.63},
    "Takoradi": {"lat": 4.89, "lon": -1.75},
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Absolute path to config.yaml on your local machine
CONFIG_PATH = os.path.expanduser(
    "~/Projects/Data-Engineering-Projects/Weather-ETL-Pipeline/src/config/config.yaml"
)

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="weather_etl_pipeline_local",
    description="Weather ETL pipeline (local version without Docker)",
    schedule='@daily',
    start_date=datetime(2025, 12, 3),
    catchup=False,
    tags=["weather", "etl"],
)
def weather_etl_pipeline():

    # -------------------------
    # 1. EXTRACT
    # -------------------------
    @task
    def extract_weather():
        all_data = {}
        for city, coords in CITIES.items():
            print(f"Extracting for {city}...")
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "hourly": "temperature_2m",
                "forecast_days": 1
            }
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            all_data[city] = response.json()
        return all_data

    # -------------------------
    # 2. TRANSFORM
    # -------------------------
    @task
    def transform_weather(raw_data: dict):
        final_records = []
        for city, data in raw_data.items():
            df = pd.DataFrame({
                "datetime": data["hourly"]["time"],
                "temperature": data["hourly"]["temperature_2m"],
            })
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["date"] = df["datetime"].dt.date
            df["city"] = city

            df_daily = df.groupby("date").agg({
                "temperature": "mean",
                "city": "first"
            }).reset_index()

            final_records.extend(df_daily.to_dict(orient="records"))
        return final_records

    # -------------------------
    # 3. LOAD
    # -------------------------
    @task
    def load_weather(records: list):
        config = load_config()
        db = config["database"]

        conn = psycopg2.connect(
            host=db["host"],
            port=db["port"],
            user=db["user"],
            password=db["password"],
            dbname=db["dbname"],
            sslmode=db.get("sslmode", "disable"),
        )

        sql = """
            INSERT INTO weather_daily (date, temperature, city)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, city)
            DO UPDATE SET temperature = EXCLUDED.temperature;
        """

        cur = conn.cursor()
        values = [(r["date"], r["temperature"], r["city"]) for r in records]
        execute_batch(cur, sql, values)
        conn.commit()
        cur.close()
        conn.close()

        print(f"Inserted {len(records)} records into Postgres.")

    # -------------------------
    # Task dependencies
    # -------------------------
    raw = extract_weather()
    transformed = transform_weather(raw)
    load_weather(transformed)

weather_etl_pipeline()


# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
# import requests
# import pandas as pd
# import json
# import psycopg2
# from psycopg2.extras import execute_batch
# import yaml


# # -----------------------------
# # CONFIG
# # -----------------------------
# CITIES = {
#     "Accra": {"lat": 5.55, "lon": -0.20},
#     "Kumasi": {"lat": 6.69, "lon": -1.63},
#     "Takoradi": {"lat": 4.89, "lon": -1.75},
# }

# BASE_URL = "https://api.open-meteo.com/v1/forecast"


# CONFIG_PATH = "/opt/airflow/src/config/config.yaml"

# def load_config():
#     with open(CONFIG_PATH, "r") as f:
#         return yaml.safe_load(f)



# # -----------------------------
# # DAG
# # -----------------------------
# @dag(
#     dag_id="weather_etl_pipeline_v1",
#     description=' Weather ETL pipeline with validation and monitoring',
#     # schedule="0 6 * * *",     # run daily at 06:00
#     schedule='@daily',
#     start_date=datetime(2025, 12,3 ),
#     catchup=False,
#     tags=["weather", "etl"],
# )
# def weather_etl_pipeline():

#     # -------------------------
#     # 1. EXTRACT
#     # -------------------------
#     @task
#     def extract_weather():
#         all_data = {}

#         for city, coords in CITIES.items():
#             print(f"Extracting for {city}...")

#             params = {
#                 "latitude": coords["lat"],
#                 "longitude": coords["lon"],
#                 "hourly": "temperature_2m",
#                 "forecast_days": 1
#             }

#             response = requests.get(BASE_URL, params=params)
#             response.raise_for_status()

#             all_data[city] = response.json()

#         # Return JSON-serialisable data for XCom
#         return all_data

#     # -------------------------
#     # 2. TRANSFORM
#     # -------------------------
#     @task
#     def transform_weather(raw_data: dict):
#         final_records = []

#         for city, data in raw_data.items():
#             df = pd.DataFrame({
#                 "datetime": data["hourly"]["time"],
#                 "temperature": data["hourly"]["temperature_2m"],
#             })

#             df["datetime"] = pd.to_datetime(df["datetime"])
#             df["date"] = df["datetime"].dt.date
#             df["city"] = city

#             df_daily = df.groupby("date").agg({
#                 "temperature": "mean",
#                 "city": "first"
#             }).reset_index()

#             # Convert to list of dicts for XCom
#             final_records.extend(df_daily.to_dict(orient="records"))

#         return final_records

#     # -------------------------
#     # 3. LOAD
#     # -------------------------
#     @task
#     def load_weather(records: list):
#         config = load_config()
#         db = config["db"]

#         conn = psycopg2.connect(
#             host=db["host"],
#             port=db["port"],
#             user=db["user"],
#             password=db["password"],
#             dbname=db["database"],
#             sslmode=db.get("sslmode", "disable"),
#         )

#         sql = """
#             INSERT INTO weather_daily (date, temperature, city)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (date, city)
#             DO UPDATE SET temperature = EXCLUDED.temperature;
#         """

#         cur = conn.cursor()

#         values = [
#             (record["date"], record["temperature"], record["city"])
#             for record in records
#         ]

#         execute_batch(cur, sql, values)
#         conn.commit()
#         cur.close()
#         conn.close()

#         print("Inserted records into Postgres.")

#     # Task dependencies
#     raw = extract_weather()
#     transformed = transform_weather(raw)
#     load_weather(transformed)


# weather_etl_pipeline()

