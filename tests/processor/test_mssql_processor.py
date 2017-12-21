import pytest
import importlib
import data_pipeline.constants.const as const
import data_pipeline.processor.factory as processor_factory

from data_pipeline.stream.mssql_message import MssqlMessage
from pytest_mock import mocker

def load_tests(name):
    # Load module which contains test data
    tests_module = importlib.import_module(name)
    # Tests are to be found in the variable `tests` of the module
    for test in tests_module.tests:
        yield test


def pytest_generate_tests(metafunc):
    """ This allows us to load tests from external files by
    parametrizing tests with each test case found in a data_X
    file """
    for fixture in metafunc.fixturenames:
        if fixture.startswith('data_'):
            # Load associated test data
            current_package = __name__.rpartition('.')[0]
            tests = load_tests("{}.{}".format(current_package, fixture))
            metafunc.parametrize(fixture, tests)


def execute_statement_parsing_test(operation, data, mocker):
    print("Running test: '{}'".format(data.description))
    
    message = MssqlMessage()
    message.operation_code = operation
    message.table_name = data.input_table_name
    message.column_names = data.input_column_names
    message.column_values = data.input_column_values
    message.primary_key_fields = data.input_primary_key_fields

    processor = processor_factory.build(const.MSSQL)
    output = processor.process(message)
    return output


def test_process_mssql_update_statements(data_mssql_updates, mocker):
    statement = execute_statement_parsing_test(const.MSSQL_UPDATE_ACTION, data_mssql_updates, mocker)
    assert statement.set_values == data_mssql_updates.expected_set_values
    print("update Statement = {}".format(str(statement)))
    assert str(statement) == data_mssql_updates.expected_sql


