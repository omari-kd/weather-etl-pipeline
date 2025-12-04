from src.etl.ingest_weather import main

if __name__ == "__main__":
    print("Starting Weather ETL from Airflow...")
    main()
    print("Weather ETL Completed!")
