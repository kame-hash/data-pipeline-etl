# data_models.py
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

class DataSourceType(Enum):
    S3 = 's3'
    LOCAL_FILE = 'local_file'

@dataclass
class DataSource:
    type: DataSourceType
    location: str
    format: str

class DataType(Enum):
    INTEGER = 'integer'
    STRING = 'string'
    FLOAT = 'float'

@dataclass
class DataColumn:
    name: str
    data_type: DataType
    is_nullable: bool = False

@dataclass
class DataTable:
    name: str
    columns: List[DataColumn]

class DataValidationError(Exception):
    pass

def validate_data_source_type(value: str) -> DataSourceType:
    try:
        return DataSourceType(value)
    except ValueError:
        raise DataValidationError(f"Invalid data source type: {value}")

def create_data_table(name: str, columns: List[Dict]) -> DataTable:
    data_columns = [DataColumn(
        name=column['name'],
        data_type=DataType(column['data_type']),
        is_nullable=column.get('is_nullable', False)
    ) for column in columns]
    return DataTable(name, data_columns)

def get_data_table_schema(table: DataTable) -> Dict:
    return {
        'name': table.name,
        'columns': [{'name': column.name, 'data_type': column.data_type.value, 'is_nullable': column.is_nullable} for column in table.columns]
    }

# Example usage:
data_table = create_data_table('orders', [
    {'name': 'id', 'data_type': 'integer'},
    {'name': 'customer_name', 'data_type': 'string', 'is_nullable': True},
    {'name': 'order_total', 'data_type': 'float'}
])

print(get_data_table_schema(data_table))