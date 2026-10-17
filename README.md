# Airflow NASA APOD ETL Pipeline

An Apache Airflow project that retrieves NASA's Astronomy Picture of the Day (APOD), transforms the API response, and loads the result into PostgreSQL. The pipeline runs daily and avoids inserting the same APOD date more than once.

## What it does

The `nasa_apod_postgres` DAG in [`dags/etl.py`](dags/etl.py) runs these tasks:

1. Creates the `apod_data` table if it does not already exist.
2. Requests the APOD record from NASA's API.
3. Selects the title, explanation, image/media URL, date, and media type.
4. Inserts the record into PostgreSQL. The date is unique, so rerunning a date does not create a duplicate.

The DAG is scheduled daily, does not backfill missed runs, and retries failed tasks twice.

## Requirements

- Docker
- The [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- A NASA API key from [api.nasa.gov](https://api.nasa.gov/) (the `DEMO_KEY` can be used for initial testing, subject to NASA's lower demo-key rate limits)

## Run locally

1. Start the PostgreSQL service:

   ```bash
   docker compose up -d postgres
   ```

   The included Compose configuration creates a local database named `postgres` and publishes PostgreSQL on port `5432`. For local development, it uses the username and password `postgres`. Do not use these default credentials in a production environment.

2. Start the Airflow project:

   ```bash
   astro dev start
   ```

   Once the services are ready, open the Airflow UI at [http://localhost:8080](http://localhost:8080) and sign in with the credentials printed by the CLI.

3. Add the Airflow connections used by the DAG in **Admin → Connections**:

   **NASA API**

   - Connection ID: `nasa_api`
   - Connection type: `HTTP`
   - Host: `https://api.nasa.gov`
   - Extra: `{"api_key": "YOUR_NASA_API_KEY"}`

   **PostgreSQL**

   - Connection ID: `my_postgres_connection`
   - Connection type: `Postgres`
   - Host: a database host reachable from the Airflow container (for example, `host.docker.internal` where supported)
   - Database/schema: `postgres`
   - Login: `postgres`
   - Password: `postgres`
   - Port: `5432`

   The PostgreSQL container must be reachable from Airflow. On Linux, if `host.docker.internal` does not resolve in your Docker setup, use the Docker host's reachable gateway address instead.

4. In the Airflow UI, enable the `nasa_apod_postgres` DAG and trigger it, or wait for its daily schedule.

To stop Airflow, run `astro dev stop`. To stop PostgreSQL while retaining its data, run `docker compose stop postgres`. The database data is stored in the `postgres_data` Docker volume.

## Verify the data

Connect to the local database with a PostgreSQL client and query the loaded rows:

```sql
SELECT date, title, media_type, url
FROM apod_data
ORDER BY date DESC;
```

## Run tests

With the Astro project running, run the DAG tests using:

```bash
astro dev pytest
```

The tests check DAG import integrity, tags, and retry configuration.

## Project layout

```text
.
├── dags/
│   └── etl.py                 # NASA APOD ETL DAG
├── tests/
│   └── dags/                  # DAG integrity tests
├── compose.yaml               # Local PostgreSQL service
├── Dockerfile                  # Astro Runtime image
└── requirements.txt           # Additional Airflow providers
```

## GitHub repository description

> A containerized Apache Airflow ETL pipeline that fetches NASA's Astronomy Picture of the Day, transforms the metadata, and stores daily records in PostgreSQL.
