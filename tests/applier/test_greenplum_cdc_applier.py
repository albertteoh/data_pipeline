import importlib
import pytest
import json
import smtplib
import tests.unittest_utils as utils
import cdc_applier_test_utils as cdc_utils
import data_pipeline.applier.factory as applier_factory
import data_pipeline.constants.const as const

from pytest_mock import mocker
from data_pipeline.stream.oracle_message import OracleMessage
from data_pipeline.processor.oracle_cdc_processor import OracleCdcProcessor
from data_pipeline.applier.greenplum_cdc_applier import GreenplumCdcApplier
from data_pipeline.sql.builder.greenplum_sql_builder import GreenplumSqlBuilder
from data_pipeline.db.db import Db


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


@pytest.fixture()
def setup(tmpdir, mocker):
    (oracle_processor,
     mock_target_db,
     mock_argv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, None, None)

    mock_target_db.dbtype = const.GREENPLUM
    gp_applier = applier_factory.build(oracle_processor, mock_target_db, mock_argv, mock_audit_factory)
    yield(gp_applier, mock_target_db, mock_audit_db)


@pytest.fixture()
def setup_metacols(tmpdir, mocker):
    metacols = { 'insert_timestamp_column': 'ctl_ins_ts', 'update_timestamp_column': 'ctl_upd_ts' }
    (oracle_processor,
     mock_target_db,
     mock_argv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, metacols, None, None)

    mock_target_db.dbtype = const.GREENPLUM
    gp_applier = applier_factory.build(oracle_processor, mock_target_db, mock_argv, mock_audit_factory)

    yield(gp_applier, mock_target_db, mock_audit_db)


def test_apply(data_greenplum_cdc_applier, mocker, setup):
    (greenplum_applier, mock_target_db, mock_audit_db) = setup
    cdc_utils.execute_tests(greenplum_applier, data_greenplum_cdc_applier, mocker, mock_target_db, mock_audit_db)

def test_apply_batch_state(data_greenplum_cdc_applier_batch_state, mocker, setup):
    (greenplum_applier, mock_target_db, mock_audit_db) = setup
    cdc_utils.execute_batch_state_tests(greenplum_applier, data_greenplum_cdc_applier_batch_state, mocker, mock_target_db)

def test_end_of_batch_without_start(mocker, setup):
    (greenplum_applier, mock_target_db, mock_audit_db) = setup

    oracle_message = OracleMessage()
    oracle_message.record_type = const.END_OF_BATCH

    config = {'value.return_value': oracle_message.serialise()}
    mock_message = mocker.Mock(**config)
   

def test_apply_metacols(data_greenplum_cdc_applier_metacols, mocker, setup_metacols):
    (postgres_applier, mock_target_db, mock_audit_db) = setup_metacols
    cdc_utils.execute_tests(postgres_applier, data_greenplum_cdc_applier_metacols, mocker, mock_target_db, mock_audit_db)


@pytest.mark.skip(reason="Performance testing")
def test_sqlalchemy_update_perf(mocker):
    import timeit
    from data_pipeline.audit.factory import AuditFactory
    from timeit import Timer

    def update_pc(mocker):
        mockargv_config = {
                'audituser': 'test/test1234@13.54.63.57:5432/iag', 
                'sourcedbtype': 'oracle',
                'sourceschema': 'ctl',
                'profileversion': '1',
                'profilename': 'myprof',
                'outputfile': '/tmp/del.txt',

        }
        mockargv = mocker.Mock(**mockargv_config)
        audit_factory = AuditFactory(mockargv)

        pc = audit_factory.build_process_control(const.CDCEXTRACT)
        pc.comment = "begin"
        pc.insert()
        pc.comment = "update"
        pc.update()

    t = Timer(lambda: update_pc(mocker))
    print t.timeit(number=50)

@pytest.mark.skip(reason="Performance testing")
def test_plainsql_update_perf(mocker):
    import psycopg2
    from timeit import Timer

    def update_pc():
        conn = psycopg2.connect(database="iag",user="test",password='test1234',host='13.54.63.57',port='5432')
        cursor = conn.cursor()

        print("inserting")
        cursor.execute("INSERT INTO process_control (id, profile_name) VALUES (nextval('process_control_id_seq'), 'myprofile')");
        conn.commit()
        print("updating")
        cursor.execute("UPDATE process_control SET comment='update' WHERE profile_name = 'myprofile'");
        conn.commit()
        cursor.close()

    t = Timer(lambda: update_pc())
    print t.timeit(number=50)
