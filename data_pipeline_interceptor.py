# data_pipeline_interceptor.py
import json
import logging
from typing import Any, Dict
from airflow.utils.log import LoggingMixin
from great_expectations import DataContext
from great_expectations.exceptions import GreatExpectationsError
from pandas import DataFrame

class DataPipelineInterceptor(LoggingMixin):
    def __init__(self, data_context: DataContext):
        self.data_context = data_context
        self.logger = logging.getLogger(__name__)

    def intercept(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Extract relevant metadata from payload
            task_id = payload['task_id']
            task_instance = payload['task_instance']
            execution_date = payload['execution_date']

            # Get data from task context
            task_context = {'task_id': task_id, 'task_instance': task_instance, 'execution_date': execution_date}
            self.logger.info(f"Task context: {task_context}")

            # Validate data against expectations
            batch = self.data_context.get_batch(task_context, batch_kwargs={'data_asset_name': 'my_data_asset'})
            self.data_context.validate(batch)

            # Update payload with validation result
            payload['validation_result'] = True

        except GreatExpectationsError as ge:
            self.logger.error(f"Validation failed: {ge}")
            payload['validation_result'] = False

        except Exception as e:
            self.logger.error(f"Interception failed: {e}")
            payload['validation_result'] = False

        return payload

    def post_execute(self, context: Dict[str, Any]) -> None:
        # Log execution metadata
        task_id = context['task_instance'].task_id
        execution_date = context['execution_date']
        self.logger.info(f"Task {task_id} executed on {execution_date}")

def main():
    data_context = DataContext()
    interceptor = DataPipelineInterceptor(data_context)

    # Example usage
    payload = {
        'task_id': 'my_task',
        'task_instance': None,
        'execution_date': '2022-01-01'
    }
    result = interceptor.intercept(payload)
    print(result)

if __name__ == "__main__":
    main()