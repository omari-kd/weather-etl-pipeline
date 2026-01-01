# Weather ETL Pipeline

A comprehensive data engineering project that extracts weather data from the Open-Meteo API, transforms it, and loads it into a PostgreSQL database using Apache Airflow for orchestration.

## Features

- **Automated ETL Pipeline**: Scheduled daily extraction of weather data for major Ghanaian cities (Accra, Kumasi, Takoradi)
- **Data Transformation**: Converts hourly weather forecasts into daily temperature averages
- **Database Integration**: Stores processed data in PostgreSQL with conflict resolution
- **Containerized Deployment**: Full Docker setup with Airflow, PostgreSQL, and Adminer
- **Local Development**: Standalone scripts for testing and development
- **Monitoring**: Airflow UI for pipeline monitoring and management

## Architecture

The pipeline consists of three main stages:

1. **Extract**: Fetches hourly temperature data from Open-Meteo API for multiple cities
2. **Transform**: Aggregates hourly data into daily averages using pandas
3. **Load**: Inserts transformed data into PostgreSQL with upsert functionality

### Project Structure

```
├── airflow/                    # Airflow configuration and DAGs
│   ├── dags/
│   │   └── weather_etl_dag.py  # Main ETL DAG
│   └── airflow.cfg
├── docker/                     # Docker-related files
├── src/                        # Source code
│   ├── config/
│   │   └── config.yaml         # Database and API configuration
│   ├── etl/                    # ETL logic modules
│   └── sql/                    # Database schema
├── scripts/                    # Utility scripts
├── weather_etl.py             # Standalone ETL script
├── docker-compose.yml         # Docker services configuration
└── requirements.txt           # Python dependencies
```

## Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)
- PostgreSQL (optional, for local setup)

## Installation

### Using Docker (Recommended)

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd weather-etl-pipeline
   ```

2. Start the services:

   ```bash
   docker-compose up -d
   ```

3. Access the services:
   - Airflow UI: http://localhost:8081 (admin/admin)
   - Adminer (DB GUI): http://localhost:8080

### Local Development

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up PostgreSQL database (or use the provided Neon connection)

3. Update `src/config/config.yaml` with your database credentials

4. Run the ETL script:
   ```bash
   python weather_etl.py
   ```

## Usage

### Running the Pipeline

The Airflow DAG is scheduled to run daily at midnight. You can also trigger it manually:

1. Open Airflow UI at http://localhost:8081
2. Find the `weather_etl_pipeline_local` DAG
3. Click the play button to trigger a manual run

### Standalone Scripts

For testing and development, use the standalone scripts:

- `weather_etl.py`: Basic ETL for Accra only
- `weather_etl2.py`: Multi-city ETL with local SQLite
- `querydata.py`: Query and analyze stored data

## Configuration

Edit `src/config/config.yaml` to configure:

- API endpoints
- Database connection parameters
- City coordinates

The config supports both local Docker PostgreSQL and cloud-hosted Neon database.

## Database Schema

```sql
CREATE TABLE weather_daily (
    date DATE NOT NULL,
    temperature FLOAT,
    city VARCHAR(50) NOT NULL,
    PRIMARY KEY (date, city)
);
```

## Technologies

- **Apache Airflow**: Workflow orchestration
- **Python**: Core programming language
- **Pandas**: Data manipulation and transformation
- **PostgreSQL**: Data storage
- **Docker**: Containerization
- **Open-Meteo API**: Weather data source
- **Requests**: HTTP client for API calls

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
