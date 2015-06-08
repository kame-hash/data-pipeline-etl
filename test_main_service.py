# data_pipeline_etl/service/test_main_service.py
import pytest
from data_pipeline_etl.service.main_service import DataPipelineService
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from pandas import DataFrame
from airflow.providers.amazon.aws.operators.s3 import S3Key
from botocore.exceptions import ClientError

@pytest.fixture
def mock_data_pipeline_service(mocker):
    return mocker.Mock(spec=DataPipelineService)

def test_extract_data_from_s3_successful(mock_data_pipeline_service):
    # Arrange
    bucket = 'my-bucket'
    key = 'data.csv'
    mock_data_pipeline_service.extract_data_from_s3.return_value = DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Bob']
    })
    # Act
    result = DataPipelineService.extract_data_from_s3(bucket, key)
    # Assert
    assert not result.empty

def test_extract_data_from_s3_unsuccessful(mock_data_pipeline_service):
    # Arrange
    bucket = 'my-bucket'
    key = 'data.csv'
    mock_data_pipeline_service.extract_data_from_s3.side_effect = ClientError(
        {'Error': {'Code': '404', 'Message': 'Not Found'}}, 'head_object')
    # Act and Assert
    with pytest.raises(ClientError):
        DataPipelineService.extract_data_from_s3(bucket, key)

def test_transform_data(mock_data_pipeline_service):
    # Arrange
    data = DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Bob']
    })
    # Act
    result = DataPipelineService.transform_data(data)
    # Assert
    assert result.equals(DataFrame({
        'id': [1, 2, 3],
        'name': ['JOHN', 'JANE', 'BOB']
    }))

def test_validate_data(mock_data_pipeline_service):
    # Arrange
    data = DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Bob']
    })
    profiler = UserConfigurableProfiler()
    # Act
    result = DataPipelineService.validate_data(data, profiler)
    # Assert
    assert result

def test_load_data_to_s3(mock_data_pipeline_service):
    # Arrange
    data = DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Bob']
    })
    bucket = 'my-bucket'
    key = 'data.csv'
    # Act
    DataPipelineService.load_data_to_s3(data, bucket, key)
    # Assert
    mock_data_pipeline_service.load_data_to_s3.assert_called_once_with(data, bucket, key)