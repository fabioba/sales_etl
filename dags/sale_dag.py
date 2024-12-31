import logging

logger = logging.getLogger(__name__)

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook


import pendulum
import pandas as pd
import io
import requests
import json

from datetime import datetime

@dag(
    schedule="@daily",
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=pendulum.datetime(2023, 1, 1),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
    },
    tags=["sale"],
)
def sale_dag():
    '''
    1. read data from the API
    2. store raw data into a bucket
    3. read raw data from bucket and load to datawarehouse
    4. move raw data to hist
    '''

    @task()
    def ingest_data_from_api():
        """"""

        # read 10 sales
        url = 'http://172.17.0.2:5000/sales?count=10'

        # read data from api
        response = requests.get(url)

        status = response.status_code

        logger.info(f'status: {status}')

        if status == 200:
            list_sales = json.loads(response.content)

            df_sales = pd.DataFrame(list_sales)

            logger.info(f'df_sales shape: {df_sales.shape}')

            # store data on google cloud storage

            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_sales.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-sales-data	')
            gcs_hook.upload(
                bucket_name= 'sales-data-raw',
                object_name= f'sales_data_{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )






    ingest_data_from_api()


sale_dag()