import confluent_kafka
import importlib
import pytest
import json
import smtplib
import cdc_applier_test_utils as cdc_utils
import data_pipeline.applier.factory as applier_factory
import data_pipeline.constants.const as const

from pytest_mock import mocker
from data_pipeline.stream.oracle_message import OracleMessage
from data_pipeline.applier.postgres_cdc_applier import PostgresCdcApplier
from data_pipeline.sql.builder.postgres_sql_builder import PostgresSqlBuilder
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
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, None, None)

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    yield(pg_applier, mock_target_db, mock_audit_db, mockargv)


@pytest.fixture()
def setup_commit_point_tests(tmpdir, mocker):
    (oracle_processor,
     mock_target_db,
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, None, None)

    at_auditcommitpoint_mock = mocker.patch.object(PostgresCdcApplier, 'at_auditcommitpoint')
    at_auditcommitpoint_mock.return_value = True

    is_end_of_batch_mock = mocker.patch('data_pipeline.applier.applier._is_end_of_batch')
    is_end_of_batch_mock.return_value = True

    end_batch_mock = mocker.patch.object(PostgresCdcApplier, '_end_batch')
    end_batch_mock.return_value = True

    can_apply_mock = mocker.patch.object(PostgresCdcApplier, '_can_apply')
    can_apply_mock.return_value = True

    oracle_message = OracleMessage()
    config = {'value.return_value': oracle_message.serialise(),
              'offset.return_value': 1}
    mock_message = mocker.Mock(**config)

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    yield(pg_applier, mock_target_db, mock_message, mock_audit_db)


@pytest.fixture()
def setup_metacols(tmpdir, mocker):
    metacols = { 'insert_timestamp_column': 'ctl_ins_ts', 'update_timestamp_column': 'ctl_upd_ts' }
    (oracle_processor,
     mock_target_db,
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, metacols, None, None)

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    yield(pg_applier, mock_target_db, mock_audit_db)


@pytest.fixture()
def setup_skipbatch(tmpdir, mocker):
    skip_batch_config = { 'skipbatch': 1 }
    (oracle_processor,
     mock_target_db,
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, None, skip_batch_config)

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    yield(pg_applier, mock_target_db, mock_audit_db)


@pytest.fixture()
def setup_bulkapply(tmpdir, mocker):
    bulk_apply_config = { 'bulkapply': True }
    (oracle_processor,
     mock_target_db,
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, None, bulk_apply_config)

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    yield(pg_applier, mock_target_db, mock_audit_db)


@pytest.fixture()
def setup_inactive_applied_tables(tmpdir, mocker):
    inactive_applied_tables = set()
    inactive_applied_tables.add('ctl.inactive_table')
    (oracle_processor,
     mock_target_db,
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, inactive_applied_tables, None)

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    yield(pg_applier, mock_target_db, mock_audit_db)


# Target commit should only be run during a batch, not after a batch is ended
def test_audit_commit_at_eob(mocker, setup_commit_point_tests):
    (postgres_applier, mock_target_db, mock_message, mock_audit_db) = setup_commit_point_tests

    audit_commit_mock = mocker.patch.object(PostgresCdcApplier, '_audit_commit')
    postgres_applier.apply(mock_message)

    audit_commit_mock.assert_not_called()


# Audit commit should only be run during a batch, not after a batch is ended
def test_target_commit_at_eob(mocker, setup_commit_point_tests):
    (postgres_applier, mock_target_db, mock_message, mock_audit_db) = setup_commit_point_tests

    target_commit_mock = mocker.patch.object(PostgresCdcApplier, '_target_commit')
    postgres_applier.apply(mock_message)

    target_commit_mock.assert_not_called()


def test_apply(data_postgres_cdc_applier, mocker, setup):
    (postgres_applier, mock_target_db, mock_audit_db, mockargv) = setup
    cdc_utils.execute_tests(postgres_applier, data_postgres_cdc_applier, mocker, mock_target_db, mock_audit_db)


def test_bulkapply(data_postgres_cdc_applier_bulkapply, mocker, setup_bulkapply):
    (postgres_applier, mock_target_db, mock_audit_db) = setup_bulkapply
    cdc_utils.execute_tests(postgres_applier, data_postgres_cdc_applier_bulkapply, mocker, mock_target_db, mock_audit_db)


def test_apply_batch_state(data_postgres_cdc_applier_batch_state, mocker, setup):
    (postgres_applier, mock_target_db, mock_audit_db, mockargv) = setup
    cdc_utils.execute_batch_state_tests(postgres_applier, data_postgres_cdc_applier_batch_state, mocker, mock_target_db)


def test_apply_skip_batch(data_postgres_cdc_applier_skipbatch, mocker, setup_skipbatch):
    (postgres_applier, mock_target_db, mock_audit_db) = setup_skipbatch
    cdc_utils.execute_tests(postgres_applier, data_postgres_cdc_applier_skipbatch, mocker, mock_target_db, mock_audit_db)


def test_inactive_applied_tables(data_postgres_cdc_applier_inactive_applied_tables, mocker, setup_inactive_applied_tables):
    (postgres_applier, mock_target_db, mock_audit_db) = setup_inactive_applied_tables
    cdc_utils.execute_tests(postgres_applier, data_postgres_cdc_applier_inactive_applied_tables, mocker, mock_target_db, mock_audit_db)


def test_apply_metacols(data_postgres_cdc_applier_metacols, mocker, setup_metacols):
    (postgres_applier, mock_target_db, mock_audit_db) = setup_metacols
    cdc_utils.execute_tests(postgres_applier, data_postgres_cdc_applier_metacols, mocker, mock_target_db, mock_audit_db)


def test_end_of_batch_without_start(mocker, setup):
    (postgres_applier, mock_target_db, mock_audit_db, mockargv) = setup

    oracle_message = OracleMessage()
    oracle_message.record_type = const.END_OF_BATCH

    config = {'value.return_value': oracle_message.serialise()}
    mock_message = mocker.Mock(**config)


def test_null_message_lsn(mocker, setup):
    (postgres_applier, mock_target_db, mock_audit_db, mockargv) = setup
    postgres_applier._maxlsns_per_table['ctl.mytable'] = '1'

    oracle_message = OracleMessage()
    oracle_message.table_name = 'mytable'

    assert postgres_applier._can_apply(oracle_message)


def test_null_max_lsn_in_ssp(mocker, setup):
    (postgres_applier, mock_target_db, mock_audit_db, mockargv) = setup
    postgres_applier._maxlsns_per_table['ctl.mytable'] = ''

    oracle_message = OracleMessage()
    oracle_message.table_name = 'mytable'
    oracle_message.commit_lsn = '1'

    assert postgres_applier._can_apply(oracle_message)


def test_default_next_offset_to_read(tmpdir, mocker):
    """Override the default setup because we want to mock out the
    the query result of _get_last_apply_record to return nothing
    """
    (oracle_processor,
     mock_target_db,
     mockargv,
     mock_audit_factory,
     mock_audit_db) = cdc_utils.setup_dependencies(tmpdir, mocker, None, None, None)

    def execute_query_se(sql, arraysize, bind_variables):
        print("Query executed: {}".format(sql))
        if "SELECT applier_marker, status" in sql:
            mock_query_results_config = {
                'fetchone.return_value': None
            }
            return mocker.Mock(**mock_query_results_config)

        raise Exception("Query '{}' is not supported in mock!".format(sql))

    mock_db_config = {'execute_query.side_effect': execute_query_se}
    mock_db = mocker.Mock(**mock_db_config)

    mock_db_factory = mocker.patch("data_pipeline.audit.factory.db_factory")
    mock_db_factory.build.return_value = mock_db

    mock_target_db.dbtype = const.POSTGRES
    pg_applier = applier_factory.build(oracle_processor, mock_target_db, mockargv, mock_audit_factory)

    offset = pg_applier.next_offset_to_read
    assert offset != confluent_kafka.OFFSET_END
    assert offset is None


@pytest.mark.parametrize("notifysummarylist, notifyerrorlist, expected_mailing_list", [
    (["a@mail.com"], [], set(["a@mail.com"])),
    ([], ["a@mail.com"], set(["a@mail.com"])),
    (["a@mail.com"], ["b@mail.com"], set(["a@mail.com", "b@mail.com"])),
])
def test_report_error(notifysummarylist, notifyerrorlist, expected_mailing_list, mocker, setup):
    (postgres_applier, mock_target_db, mock_audit_db, mockargv) = setup

    mockargv.notifyerrorlist = notifyerrorlist
    mockargv.notifysummarylist = notifysummarylist

    error_message = "Oops"
    mailer_mock = mocker.patch('data_pipeline.applier.applier.mailer')

    postgres_applier.report_error(error_message)
    mailer_mock.send.assert_called_once_with(
        'someone',
        expected_mailing_list,
        'myprofile applier has failed. Partial batch (up to error) committed.',
        'localhost',
        plain_text_message='Error: Oops\n***STATE DUMP***\n\nLast executed state: {}\n\nLast committed state: None\n\nCurrent message: None\n\n'
    )

   
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
