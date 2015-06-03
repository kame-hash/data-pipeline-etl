# utils/helpers.py
import pandas as pd
import logging
from great_expectations import DataContext
from airflow.providers.amazon.aws.transfers.s3 import S3Key
from airflow.exceptions import AirflowException

def load_data_from_s3(bucket_name: str, key: str) -> pd.DataFrame:
    try:
        s3_key = S3Key(key, bucket_name=bucket_name)
        data = s3_key.read_csv()
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Failed to load data from S3: {e}")
        raise AirflowException(f"Failed to load data from S3: {e}")

def validate_data(data: pd.DataFrame, expectation_suite_name: str) -> bool:
    context = DataContext()
    suite = context.get_expectation_suite(expectation_suite_name)
    validation_result = context.validate(data, suite)
    if not validation_result.success:
        logging.error(f"Data validation failed: {validation_result.summary()")
        return False
    return True

def write_data_to_s3(bucket_name: str, key: str, data: pd.DataFrame) -> None:
    try:
        s3_key = S3Key(key, bucket_name=bucket_name)
        s3_key.write_csv(data, index=False)
    except Exception as e:
        logging.error(f"Failed to write data to S3: {e}")
        raise AirflowException(f"Failed to write data to S3: {e}")

def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    # apply transformations to the data
    data['date'] = pd.to_datetime(data['date'])
    data['amount'] = data['amount'].astype(float)
    return data

def get_data_context() -> DataContext:
    return DataContext()

def validate_s3_key_exists(bucket_name: str, key: str) -> bool:
    try:
        s3_key = S3Key(key, bucket_name=bucket_name)
        if s3_key.check_if_exists():
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Failed to check if S3 key exists: {e}")
        return False

def get_s3_key_last_modified(bucket_name: str, key: str) -> str:
    try:
        s3_key = S3Key(key, bucket_name=bucket_name)
        return s3_key.last_modified
    except Exception as e:
        logging.error(f"Failed to get last modified date of S3 key: {e}")
        return None