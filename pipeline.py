# src/services/pipeline.py
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from great_expectations import DataContext
import pandas as pd
from pipeline_config import S3_SOURCE_BUCKET, S3_SINK_BUCKET, EXPECTATIONS_SUITE

class PipelineService:
    def __init__(self, dag: DAG):
        self.dag = dag
        self.logger = logging.getLogger(__name__)

    def extract_from_s3(self, execution_date):
        # use a S3KeySensor to wait for the data to be available
        s3_sensor = S3KeySensor(
            task_id='wait_for_data',
            bucket_key=f'{S3_SOURCE_BUCKET}/{execution_date.strftime("%Y/%m/%d")}/data.csv',
            wildcard_match=False,
            timeout=18*60*60,  # 18 hours
            poke_interval=60,  # 1 minute
            dag=self.dag
        )
        return s3_sensor

    def transform_data(self, execution_date, **kwargs):
        # read the data from S3
        s3_file = f'{S3_SOURCE_BUCKET}/{execution_date.strftime("%Y/%m/%d")}/data.csv'
        df = pd.read_csv(f's3://{s3_file}')

        # validate the data with Great Expectations
        context = DataContext()
        validation_result = context.validate(df, EXPECTATIONS_SUITE)
        if not validation_result.success:
            self.logger.error(f'Data validation failed: {validation_result.message}')
            raise ValueError('Data validation failed')

        # apply some simple transformations
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['value'] = df['value'] * 2  # just some example transformation

        # write the transformed data back to S3
        s3_sink_file = f'{S3_SINK_BUCKET}/{execution_date.strftime("%Y/%m/%d")}/transformed_data.csv'
        df.to_csv(f's3://{s3_sink_file}', index=False)

        return s3_sink_file

    def load_toSink(self, execution_date):
        # use a PythonOperator to execute the transform_data function
        transform_task = PythonOperator(
            task_id='transform_data',
            python_callable=self.transform_data,
            op_kwargs={'execution_date': execution_date},
            dag=self.dag
        )
        return transform_task

def create_dag():
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    with DAG(
        'data_pipeline',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
    ) as dag:
        pipeline_service = PipelineService(dag)
        pipeline_service.extract_from_s3(dag.default_args['execution_date'])
        pipeline_service.load_toSink(dag.default_args['execution_date'])

create_dag()