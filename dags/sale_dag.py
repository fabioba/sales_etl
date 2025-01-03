import logging

logger = logging.getLogger(__name__)

from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

import pendulum
import pandas as pd
import io
from resources.sales_data import generate_sales
from resources.utils import get_query

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

    @task_group(group_id='extract_load_bgq')
    def extract_load_bgq():
            
        @task()
        def ingest_data_from_api():
            """"""
            list_sales = generate_sales()

            df_sales = pd.DataFrame(list_sales)

            logger.info(f'df_sales shape: {df_sales.shape}')

            # store data on google cloud storage

            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_sales.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Upload CSV to GCS
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-sales-data')
            gcs_hook.upload(
                bucket_name= 'sales-data-raw',
                object_name= f'raw/sales_data_{datetime.now().strftime("%m_%d_%Y_%H:%M:%S")}.csv',
                data=csv_buffer.getvalue(),
                mime_type='text/csv',
            )


        @task()
        def load_data_dwh():
            gcs_hook = GCSHook(gcp_conn_id= 'gcp-sales-data')

            list_raw_files = gcs_hook.list(bucket_name='sales-data-raw', prefix='raw/')

            for raw_file in list_raw_files:

                logger.info(f'read raw_file: {raw_file}')

                # Initialize the BigQuery Hook
                bq_hook = BigQueryHook(gcp_conn_id= 'gcp-sales-data')

                # Download the file as a string
                file_content = gcs_hook.download(bucket_name='sales-data-raw', object_name=raw_file)
                
                # Convert the content to a pandas DataFrame
                df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))

                rows = df.to_dict(orient="records")

                # Insert rows into the BigQuery table
                bq_hook.insert_all(
                    project_id= 'ace-mile-446412-j2',
                    dataset_id= 'SALES',
                    table_id= 'RAW_SALES',
                    rows=rows,
                )

        gcs_raw_to_hist = GCSToGCSOperator(
                task_id="gcs_raw_to_hist",
                source_bucket="sales-data-raw",
                source_objects= ["raw/*"],
                destination_bucket="sales-data-raw",
                destination_object="hist/",
                gcp_conn_id= 'gcp-sales-data',
                move_object = True
            )    

        ingest_data_from_api() >> load_data_dwh() >> gcs_raw_to_hist


    @task_group(group_id='transform_bgq')
    def transform_bgq():
        
        @task_group(group_id='ext')
        def ext():

            truncate_ext_raw_sales = BigQueryInsertJobOperator(
                task_id="truncate_ext_raw_sales",
                configuration={
                    "query": {
                        "query": get_query('include/sql/ext/truncate_ext_raw_sales.sql'),
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id="gcp-sales-data",  # Replace if using a custom connection
            )

            insert_ext_raw_sales = BigQueryInsertJobOperator(
                task_id="insert_ext_raw_sales",
                configuration={
                    "query": {
                        "query": get_query('include/sql/ext/insert_ext_raw_sales.sql'),
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id="gcp-sales-data",  # Replace if using a custom connection
            )

            truncate_ext_raw_sales >> insert_ext_raw_sales

        @task_group(group_id='dim')
        def dim():

            dim_product = BigQueryInsertJobOperator(
                task_id="dim_product",
                configuration={
                    "query": {
                        "query": get_query('include/sql/dim/dim_product.sql'),
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id="gcp-sales-data",  # Replace if using a custom connection
            )

            dim_customer = BigQueryInsertJobOperator(
                task_id="dim_customer",
                configuration={
                    "query": {
                        "query": get_query('include/sql/dim/dim_customer.sql'),
                        "useLegacySql": False,
                    }
                },
                gcp_conn_id="gcp-sales-data",  # Replace if using a custom connection
            )


            dim_product >> dim_customer
        

        fct_sales = BigQueryInsertJobOperator(
            task_id="fct_sales",
            configuration={
                "query": {
                    "query": get_query('include/sql/fct/fct_sales.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-sales-data",  # Replace if using a custom connection
        )

        update_cfg_flow_manager = BigQueryInsertJobOperator(
            task_id="update_cfg_flow_manager",
            configuration={
                "query": {
                    "query": get_query('include/sql/update_cfg_flow_manager.sql'),
                    "useLegacySql": False,
                }
            },
            gcp_conn_id="gcp-sales-data",  # Replace if using a custom connection
        )

        ext() >> dim() >> fct_sales >> update_cfg_flow_manager
        

    extract_load_bgq() >> transform_bgq()

sale_dag()