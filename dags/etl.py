# # Extract and Transform and Load
# from datetime import timedelta

# from airflow import DAG
# from airflow.decorators import task
# from airflow.providers.http.operators.http import HttpOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils import timezone


# # Define the DAG

# with DAG(
#     dag_id="nasa_apod_postgres",
#     start_date=timezone.utcnow() - timedelta(days=1),
#     schedule="@daily",
#     catchup=False,
#     default_args={"retries": 2},
#     tags=["nasa", "etl"],
# ) as dag:
#     ## Step 1- Create the table if it does not exists

#     @task
#     def create_table():
#         ## initialize the PostgresHook
#         postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

#         ## SQL query to create the table
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS apod_data (
#             id SERIAL PRIMARY KEY,
#             title VARCHAR(255),
#             explanation TEXT,
#             url TEXT,
#             date DATE,
#             media_type VARCHAR(50)
#         )
#         """

#         # Execute the table creation queary
#         postgres_hook.run(create_table_query)

#     ## Sept 2 - Extract the NASA API Data(APOD) - astronomy Picture of the DAY
#     extract_apod = HttpOperator(
#         task_id="extract_apod",
#         http_conn_id="nasa_api",  ## Connection ID Defined In Airflow For NASA API
#         endpoint="planetary/apod",  ## NASA API enpoint for APOD
#         method="GET",
#         data={
#             "api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"
#         },  ## Use the API Key from the connection
#         response_filter=lambda response: response.json(),  ## Convert response to json
#     )

#     ## Step 3 - Tranform the data(Pick the information that i need to save)
#     @task
#     def transform_apod_data(response):
#         apod_data = {
#             "title": response.get("title", ""),
#             "explanation": response.get("explanation", ""),
#             "url": response.get("url", ""),
#             "date": response.get("date", ""),
#             "media_type": response.get("media_type", ""),
#         }
#         return apod_data

#     ## step 4:  Load the data into Postgres SQL
#     @task
#     def load_data_to_postgres(apod_data):
#         ## Initialize the PostgresHook
#         postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

#         ## Define the SQL Insert Query

#         insert_query = """
#         INSERT INTO apod_data (title, explanation, url, date, media_type)
#         VALUES (%s, %s, %s, %s, %s);
#         """

#         ## Execute the SQL Query

#         postgres_hook.run(
#             insert_query,
#             parameters=(
#                 apod_data["title"],
#                 apod_data["explanation"],
#                 apod_data["url"],
#                 apod_data["date"],
#                 apod_data["media_type"],
#             ),
#         )
        
#     ## step 5: Verify the data DBViewer
    
        
#     ## step 6: Define the task dependencies
#         ## Extract
#     create_table() >> extract_apod  ## Ensure the table is create befor extraction
#     api_response=extract_apod.output
#         ## Transform
#     transformed_data=transform_apod_data(api_response)
#         ## Load
#     load_data_to_postgres(transformed_data)
    
        
# NASA APOD: Extract, Transform, Load

import pendulum

from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="nasa_apod_postgres",
    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2},
    tags=["nasa", "etl"],
) as dag:

    # Step 1: Create the PostgreSQL table
    @task
    def create_table():
        postgres_hook = PostgresHook(
            postgres_conn_id="my_postgres_connection"
        )

        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE UNIQUE,
            media_type VARCHAR(50)
        );
        """

        postgres_hook.run(create_table_query)

    # Step 2: Extract NASA Astronomy Picture of the Day
    extract_apod = HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api",
        endpoint="planetary/apod",
        method="GET",
        data={
            "api_key": (
                "{{ conn.nasa_api.extra_dejson.api_key }}"
            )
        },
        response_filter=lambda response: response.json(),
        log_response=False,
    )

    # Step 3: Transform the API response
    @task
    def transform_apod_data(response: dict) -> dict:
        return {
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""),
            "date": response.get("date", ""),
            "media_type": response.get("media_type", ""),
        }

    # Step 4: Load data into PostgreSQL
    @task
    def load_data_to_postgres(apod_data: dict):
        postgres_hook = PostgresHook(
            postgres_conn_id="my_postgres_connection"
        )

        insert_query = """
        INSERT INTO apod_data (
            title,
            explanation,
            url,
            date,
            media_type
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
        """

        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data["title"],
                apod_data["explanation"],
                apod_data["url"],
                apod_data["date"],
                apod_data["media_type"],
            ),
        )

    # Step 5: Define dependencies
    table_created = create_table()

    api_response = extract_apod.output

    transformed_data = transform_apod_data(api_response)

    data_loaded = load_data_to_postgres(transformed_data)

    table_created >> extract_apod >> transformed_data >> data_loaded
    
