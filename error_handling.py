# error_handling.py
import logging
from airflow.exceptions import AirflowException
from great_expectations.exceptions import GreatExpectationsError
from botocore.exceptions import ClientError

class DataPipelineError(AirflowException):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)

class S3OperationError(DataPipelineError):
    def __init__(self, msg, operation, bucket, key):
        self.operation = operation
        self.bucket = bucket
        self.key = key
        super().__init__(f"S3 {operation} error: {msg} - bucket: {bucket}, key: {key}")

class DataValidationError(DataPipelineError):
    def __init__(self, msg, expectation_suite_name, validation_result):
        self.expectation_suite_name = expectation_suite_name
        self.validation_result = validation_result
        super().__init__(f"Data validation error in {expectation_suite_name}: {msg}")

class DataTransformError(DataPipelineError):
    def __init__(self, msg, transform_name):
        self.transform_name = transform_name
        super().__init__(f"Data transform {transform_name} error: {msg}")

def handle_s3_error(e, operation, bucket, key):
    if isinstance(e, ClientError):
        error_msg = f"Error code: {e.response['Error']['Code']}, Message: {e.response['Error']['Message']}"
        logging.error(error_msg)
        raise S3OperationError(error_msg, operation, bucket, key)
    else:
        raise e

def handle_data_validation_error(e, expectation_suite_name, validation_result):
    if isinstance(e, GreatExpectationsError):
        error_msg = f"Validation result: {validation_result}"
        logging.error(error_msg)
        raise DataValidationError(error_msg, expectation_suite_name, validation_result)
    else:
        raise e

def handle_transform_error(e, transform_name):
    if isinstance(e, Exception):
        error_msg = f"Error in transform {transform_name}: {str(e)}"
        logging.error(error_msg)
        raise DataTransformError(error_msg, transform_name)
    else:
        raise e

try:
    # example usage of error handling
    s3_operation("upload", "my-bucket", "my-key")
except Exception as e:
    handle_s3_error(e, "upload", "my-bucket", "my-key")

try:
    # example usage of data validation error handling
    validation_result = validate_data("my_expectation_suite")
    if not validation_result.success:
        raise GreatExpectationsError("Data validation failed")
except Exception as e:
    handle_data_validation_error(e, "my_expectation_suite", validation_result)

try:
    # example usage of transform error handling
    transformed_data = apply_transform("my_transform", data)
except Exception as e:
    handle_transform_error(e, "my_transform")