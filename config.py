# config.py
import os
import logging
from airflow import settings
from great_expectations import DataContext
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.amazon.aws.transfers.s3 import S3Key

DEFAULT_DATA_DIR = 'data'
AWS_CONNECTION_ID = 'aws_default'
S3_BUCKET_NAME = 'my-bucket'
EXPECTATIONS_DIR = 'expectations'
VALIDATED_DATA_DIR = 'validated_data'

def get_data_directory():
    return os.path.join(settings.AIRFLOW_HOME, DEFAULT_DATA_DIR)

def get_s3_bucket_name():
    return S3_BUCKET_NAME

def get_expectations_directory():
    return EXPECTATIONS_DIR

def get_validated_data_directory():
    return VALIDATED_DATA_DIR

def init_great_expectations_context(expectations_dir):
    try:
        return.DataContext(context_root_dir=expectations_dir)
    except Exception as e:
        logging.error(f"Failed to initialize Great Expectations context: {e}")
        return None

def load_s3_config(aws_connection_id):
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    hook = AwsBaseHook(aws_conn_id=aws_connection_id)
    aws_config = hook.get_connection(aws_connection_id)
    if aws_config:
        return aws_config.extra_dejson
    else:
        logging.error("AWS connection not found")
        return None

def create_s3_key(s3_bucket_name, s3_key):
    return S3Key(s3_bucket_name, s3_key)

class DataConfig(AirflowPlugin):
    name = 'data_config'
    hooks = []
    operators = []
    executors = []

    def __init__(self, data_dir, s3_bucket_name, expectations_dir, validated_data_dir):
        self.data_dir = data_dir
        self.s3_bucket_name = s3_bucket_name
        self.expectations_dir = expectations_dir
        self.validated_data_dir = validated_data_dir
        self.great_expectations_context = init_great_expectations_context(self.expectations_dir)
        self.s3_config = load_s3_config(AWS_CONNECTION_ID)

def get_config():
    data_dir = get_data_directory()
    s3_bucket_name = get_s3_bucket_name()
    expectations_dir = get_expectations_directory()
    validated_data_dir = get_validated_data_directory()
    return DataConfig(data_dir, s3_bucket_name, expectations_dir, validated_data_dir)

config = get_config()