import datetime
import importlib
import logging
import os
import pytest
import yaml
import tests.unittest_utils as unittest_utils

import data_pipeline.constants.const as const
import data_pipeline.initsync.factory as db_factory
import data_pipeline.initsync_pipe as initsync_pipe
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.utils.utils as utils

from .data_build_colname_sql import (
    ORACLE_SOURCE_DEF_COLNAME_SQL,
    ORACLE_TARGET_DEF_COLNAME_SQL,
    ORACLE_SOURCE_DEF_IGNORED_COLNAME_SQL,
    ORACLE_TARGET_DEF_IGNORED_COLNAME_SQL,

    MSSQL_SOURCE_DEF_COLNAME_SQL,
    MSSQL_TARGET_DEF_COLNAME_SQL,
    MSSQL_SOURCE_DEF_IGNORED_COLNAME_SQL,
    MSSQL_TARGET_DEF_IGNORED_COLNAME_SQL,

    POSTGRES_SOURCE_DEF_COLNAME_SQL,
    POSTGRES_TARGET_DEF_COLNAME_SQL,
    POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
    POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,

    GREENPLUM_SOURCE_DEF_COLNAME_SQL,
    GREENPLUM_TARGET_DEF_COLNAME_SQL,
    GREENPLUM_SOURCE_DEF_IGNORED_COLNAME_SQL,
    GREENPLUM_TARGET_DEF_IGNORED_COLNAME_SQL,
)

from .data_table_exists import (ORACLE_TABLE_EXISTS_SQL,
                                POSTGRES_TABLE_EXISTS_SQL,
                                MSSQL_TABLE_EXISTS_SQL)


from mock import Mock, MagicMock
from data_pipeline.initsync.exceptions import NotSupportedException
from data_pipeline.audit.custom_orm import (ProcessControl,
                                            ProcessControlDetail,
                                            SourceSystemProfile)
from data_pipeline.sql.table_name import TableName

COLNAME_QUERIES = [
    ORACLE_SOURCE_DEF_COLNAME_SQL,
    MSSQL_SOURCE_DEF_COLNAME_SQL,
    POSTGRES_SOURCE_DEF_COLNAME_SQL,
    GREENPLUM_SOURCE_DEF_COLNAME_SQL,
    ORACLE_TARGET_DEF_COLNAME_SQL,
    MSSQL_TARGET_DEF_COLNAME_SQL,
    POSTGRES_TARGET_DEF_COLNAME_SQL,
    GREENPLUM_TARGET_DEF_COLNAME_SQL,
]


IGNORED_COLNAME_QUERIES = [
    ORACLE_SOURCE_DEF_IGNORED_COLNAME_SQL,
    MSSQL_SOURCE_DEF_IGNORED_COLNAME_SQL,
    POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
    GREENPLUM_SOURCE_DEF_IGNORED_COLNAME_SQL,
    ORACLE_TARGET_DEF_IGNORED_COLNAME_SQL,
    MSSQL_TARGET_DEF_IGNORED_COLNAME_SQL,
    POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
    GREENPLUM_TARGET_DEF_IGNORED_COLNAME_SQL,
]

# field_name, data_type, size, precision, scale 
MOCK_SOURCE_DB_COLUMNS = [
    ['col1', 'VARCHAR', '10', None, None],
    ['/col2\\', 'NUMBER', None, '3', '0'],
    ['bitcol', 'BIT', None, None, None],
    ['textcol', 'TEXT', None, None, None],
    ['from', 'INTEGER', None, None, None], # reserved word colname
    ['fromm', 'INTEGER', None, None, None], # looks like a reserved word
    ['to_', 'INTEGER', None, None, None], # test that we don't modify this
]


# intentionally missing col1
# field_name, data_type, size, precision, scale 
MOCK_TARGET_DB_COLUMNS = [
    ['/col2\\', 'NUMBER', None, '3', '0'],
    ['bitcol', 'BIT', None, None, None],
    ['textcol', 'TEXT', None, None, None],
    ['from_', 'INTEGER', None, None, None], # reserved word colname
    ['fromm', 'INTEGER', None, None, None], # looks like a reserved word
    ['to', 'INTEGER', None, None, None], # test that we don't modify this
]


MOCK_DB_IGNORED_COLUMNS = [
    ['ignoredcol0', 'INTEGER'],
    ['ignoredcol1', 'INTEGER'],
]


MOCK_DB_PK_COLUMNS = [('mytable', 'bitcol'), ('mytable', 'textcol')]


TEST_AUDIT_SCHEMA = 'foo'


BASE_KEY_COLUMN_SQL_SNIPPET = "FROM information_schema.key_column_usage"
ORACLE_KEY_COLUMN_SQL_SNIPPET = "FROM all_constraints cons, all_cons_columns col"
ALL_KEY_COLUMN_SQL_SNIPPETS = [BASE_KEY_COLUMN_SQL_SNIPPET, ORACLE_KEY_COLUMN_SQL_SNIPPET]


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


def source_db_execute_query_se(query, arraysize, values=(),
                               post_process_func=None):
    """Mocks the response from calling execute_query on a source database"""
    print("Source DB query executed='{}'".format(query))

    if (ORACLE_TABLE_EXISTS_SQL == query or
        MSSQL_TABLE_EXISTS_SQL == query):
        mock_query_results_config = {
            'next.return_value': [1],
            'fetchone.return_value': [1]
        }
        return Mock(**mock_query_results_config)
    elif POSTGRES_TABLE_EXISTS_SQL == query:
        mock_query_results_config = {
            'next.return_value': [True],
            'fetchone.return_value': [True]
        }
        return Mock(**mock_query_results_config)

    elif query in COLNAME_QUERIES:
        return MOCK_SOURCE_DB_COLUMNS + MOCK_DB_IGNORED_COLUMNS
    elif query in IGNORED_COLNAME_QUERIES:
        return MOCK_SOURCE_DB_COLUMNS
    elif is_keycolumnlist_query(query):
        return MOCK_DB_PK_COLUMNS
    elif "FROM myschema.mytable" in query:
        return [
            ('0', 0, 0, '0')
        ]
    else:
        raise Exception("Unsupported query: {}".format(query))


def target_db_execute_query_se(query, arraysize, values=(),
                               post_process_func=None):
    """Mocks the response from calling execute_query on a target database
    Note: this differs from the above source_db_execute_query_se by
    omitting the col1 column to simulate a differing schema"""

    print("Target DB query executed='{}'".format(query))

    if (ORACLE_TABLE_EXISTS_SQL == query or
        MSSQL_TABLE_EXISTS_SQL == query):
        mock_query_results_config = {
            'next.return_value': [1],
            'fetchone.return_value': [1]
        }
        return Mock(**mock_query_results_config)
    elif POSTGRES_TABLE_EXISTS_SQL == query:
        mock_query_results_config = {
            'next.return_value': [True],
            'fetchone.return_value': [True]
        }
        return Mock(**mock_query_results_config)

    elif query in COLNAME_QUERIES:
        return MOCK_TARGET_DB_COLUMNS + MOCK_DB_IGNORED_COLUMNS
    elif query in IGNORED_COLNAME_QUERIES:
        return MOCK_TARGET_DB_COLUMNS
    elif is_keycolumnlist_query(query):
        return MOCK_DB_PK_COLUMNS
    elif "FROM myschema.mytable" in query:
        return [
            ('0', 0, 0, '0')
        ]
    else:
        raise Exception("Unsupported query: {}".format(query))


def is_keycolumnlist_query(query):
    for sql_snippet in ALL_KEY_COLUMN_SQL_SNIPPETS:
        if sql_snippet in query:
            return True
    return False


def build_mock_db(mocker, base_config, dbtype):
    mock_source_db_config = utils.merge_dicts(base_config, {
        'dbtype': dbtype
    })
    return mocker.Mock(**mock_source_db_config)


mock_db_cache = {}
def build_initsync_db(mocker, mock_db, argv, logger):
    mock_db_cache[mock_db.dbtype] = mock_db

    def mock_db_factory_build_se(dbtype_name):
        print("Building {} DB".format(dbtype_name))
        return mock_db_cache[dbtype_name]

    mock_db_factory_build = mocker.patch('data_pipeline.initsync.factory.db_factory.build')
    mock_db_factory_build.side_effect = mock_db_factory_build_se

    return db_factory.build(mock_db.dbtype, argv, logger, const.SOURCE)


@pytest.fixture()
def setup(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'auditschema': TEST_AUDIT_SCHEMA
    })
    mockargv = mocker.Mock(**mockargv_config)

    unittest_utils.setup_logging(mockargv.workdirectory)

    mock_get_program_args = mocker.patch(
        'data_pipeline.initsync_pipe.get_program_args')
    mock_get_program_args.return_value = mockargv

    mock_source_db_config = {
        'execute_query.side_effect': source_db_execute_query_se,
        'close.return_value': None,
    }
    mock_target_db_config = {
        'execute_query.side_effect': target_db_execute_query_se,
        'close.return_value': None,
    }

    logger = logging.getLogger(__name__)

    table = TableName('myschema', 'mytable')

    mock_process_control_constructor(mocker)
    mock_process_control_detail_constructor(mocker)

    yield (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir)


def mock_process_control_constructor(mocker):
    mock_pc = build_mock_pc(mocker)
    mock_pc_constructor = mocker.patch(
        'data_pipeline.initsync_pipe.ProcessControl')
    mock_pc_constructor.return_value = mock_pc


def build_mock_pc(mocker):
    mock_pc_config = { }
    return mocker.Mock(**mock_pc_config)


def mock_process_control_detail_constructor(mocker):
    mock_pcd = build_mock_pcd(mocker)
    mock_pcd_constructor = mocker.patch(
        'data_pipeline.initsync_pipe.ProcessControlDetail')
    mock_pcd_constructor.return_value = mock_pcd


def build_mock_pcd(mocker):
    mock_pcd_config = {
        'insert.return_value': None,
        'update.return_value': None,
    }
    return mocker.Mock(**mock_pcd_config)


def test_table_exists(data_table_exists, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    dbtype = data_table_exists.input_dbtype
    expected_sql_call = data_table_exists.expected_sql_call 

    mock_db = build_mock_db(mocker, mock_source_db_config, dbtype)
    db = build_initsync_db(mocker, mock_db, mockargv, logger)

    assert db.table_exists(table)
    mock_db.execute_query.assert_called_once_with(
        expected_sql_call, const.DEFAULT_ARRAYSIZE)


def test_build_colname_sql(data_build_colname_sql, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    print("data_build_colname_sql={}".format(data_build_colname_sql))
    source_dbtype = data_build_colname_sql.input_source_dbtype
    target_dbtype = data_build_colname_sql.input_target_dbtype
    ignored_columns = data_build_colname_sql.input_ignored_columns
    nonstandardcolumnnames = data_build_colname_sql.input_nonstandardcolumnnames
    definition_origin = data_build_colname_sql.input_definition_origin
    expected_sql_calls_on_definition_db = data_build_colname_sql.expected_sql_calls_on_definition_db
    expected_source_cols = data_build_colname_sql.expected_source_cols
    expected_target_cols = data_build_colname_sql.expected_target_cols

    mockargv_config = utils.merge_dicts(mockargv_config, {
        "metacols": utils.merge_dicts(
            mockargv_config["metacols"], {
                const.METADATA_IGNORED_COLS: ignored_columns,
            }
        ),
        "nonstandardcolumnnames": nonstandardcolumnnames,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_source_db = build_mock_db(mocker, mock_source_db_config, source_dbtype)
    source_db = build_initsync_db(mocker, mock_source_db, mockargv, logger)

    mock_target_db = build_mock_db(mocker, mock_target_db_config, target_dbtype)
    target_db = build_initsync_db(mocker, mock_target_db, mockargv, logger)

    if expected_sql_calls_on_definition_db:
        (source_cols,
         target_cols,
         keycolumnlist) = initsync_pipe.get_column_lists_by_definition_origin(
            source_db, table,
            target_db, table,
            definition_origin)

        mock_definition_db = mock_source_db
        if definition_origin == const.TARGET:
            mock_definition_db = mock_target_db

        mock_definition_db.assert_has_calls(
            [mocker.call.execute_query(sql, const.DEFAULT_ARRAYSIZE)
             for sql in expected_sql_calls_on_definition_db]
        )

        print("source_cols:\nactual={}\nexpect={}".format(source_cols, expected_source_cols))
        print("targte_cols:\nactual={}\nexpect={}".format(target_cols, expected_target_cols))
        assert source_cols == expected_source_cols
        assert target_cols == expected_target_cols
    else:
        with pytest.raises(NotSupportedException) as e:
            db.get_column_list(table, strip_control_chars)


def test_initsync_table(mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    mockargv_config = utils.merge_dicts(mockargv_config, {
        "sourcedbtype": const.ORACLE,
        "targetdbtype": const.POSTGRES,
    })
    mockargv = mocker.Mock(**mockargv_config)

    source_conn_details = dbuser.get_dbuser_properties(mockargv.sourceuser)
    target_conn_details = dbuser.get_dbuser_properties(mockargv.targetuser)
    source_schema = "myschema"
    target_schema = "myschema"
    process_control_id = 1
    query_condition = None

    mock_msg_queue_config = {"put.return_value": None}
    mock_msg_queue = mocker.Mock(**mock_msg_queue_config)

    mock_source_db = build_mock_db(mocker, mock_source_db_config, const.ORACLE)
    source_db = build_initsync_db(mocker, mock_source_db, mockargv, logger)

    mock_target_db = build_mock_db(mocker, mock_source_db_config, const.POSTGRES)
    target_db = build_initsync_db(mocker, mock_target_db, mockargv, logger)

    mock_update_ssp = mocker.patch("data_pipeline.initsync_pipe._update_source_system_profile")
    # We don't want a real process being forked during tests
    mock_process = mocker.patch("data_pipeline.initsync_pipe.multiprocessing.Process")

    initsync_pipe.initsync_table(mockargv, source_conn_details,
        target_conn_details, source_schema, table.name, target_schema,
        process_control_id, query_condition, mock_msg_queue)

    assert mock_update_ssp.call_count == 2

DEFAULT_METACOLS_SELECT = """
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp"""

LSN_UNSUPPORTED = "UNSUPPORTED BY DB"

def test_extract(data_extract, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    dbtype = data_extract.dbtype
    directunload = data_extract.directunload
    extractlsn = data_extract.extractlsn
    metacols = data_extract.metacols
    raise_exception = data_extract.raise_exception
    lsn_select = data_extract.lsn_select
    expected_sql = data_extract.expected_sql

    mockargv_config = utils.merge_dicts(mockargv_config, {
        "directunload": directunload,
        "extractlsn": extractlsn,
        "metacols": metacols,
        "samplerows": 10,
    })
    mockargv = mocker.Mock(**mockargv_config)

    result_row = ['a', 'b', 'c']
    mock_lsn = None
    now_str = datetime.datetime.now().strftime(const.TIMESTAMP_FORMAT)

    append_metacols = DEFAULT_METACOLS_SELECT if metacols else const.EMPTY_STRING
    append_lsn_select = """
            , '{}'""".format(lsn_select) if extractlsn else const.EMPTY_STRING

    if mockargv.metacols:
        inscol = mockargv.metacols.get(const.METADATA_INSERT_TS_COL)
        if inscol:
            result_row.append(now_str)

        updcol = mockargv.metacols.get(const.METADATA_UPDATE_TS_COL)
        if inscol:
            result_row.append(now_str)

    if mockargv.extractlsn:
        result_row.append(mock_lsn)
        mock_lsn = 333

    def execute_query_se(query, arraysize, post_process_func):
        mock_query_results_config = {
            "fetchmany.return_value": [result_row],
        }

        mock_query_results = mocker.Mock(**mock_query_results_config)
        print("Executing query: {}".format(query))
        return mock_query_results

    mock_source_db_config = utils.merge_dicts(mock_source_db_config, {
        "copy_expert.return_value": 99,
        "execute_query.side_effect": execute_query_se,
    })

    def log_func_se(argv, table, extract_data_sql):
        if raise_exception:
            raise Exception("FOO!")

    mock_log_func = mocker.patch(
        "data_pipeline.initsync_pipe._log_extract_data_sql")
    mock_log_func.side_effect = log_func_se

    mock_extract_data = mocker.patch(
        "data_pipeline.initsync_pipe.ProcessControlDetail")

    mock_db = build_mock_db(mocker, mock_source_db_config, dbtype)
    db = build_initsync_db(mocker, mock_db, mockargv, logger)

    pipe_file = os.path.join(mockargv.workdirectory, "myfakefifo")
    open(pipe_file, 'a').close()

    mock_pc_detail_config = {"update.return_value":None}
    mock_pc_detail = mocker.Mock(**mock_pc_detail_config)
    mock_pc_detail_cons = mocker.patch(
        "data_pipeline.initsync_pipe.ProcessControlDetail")
    mock_pc_detail_cons.return_value = mock_pc_detail

    mock_msg_queue_config = {"put.return_value": None}
    mock_msg_queue = mocker.Mock(**mock_msg_queue_config)

    out = "foo"
    err = "bar"

    mock_cmd_return_config = {
        "communicate.return_value": (out, err),
        "returncode": 1 if raise_exception else 0,
    }

    mock_cmd_return = mocker.Mock(**mock_cmd_return_config)
    mock_subprocess = mocker.patch(
        "data_pipeline.initsync_pipe.subprocess")
    mock_subprocess.Popen.return_value = mock_cmd_return

    source_conn_details = dbuser.get_dbuser_properties(mockargv.sourceuser)
    initsync_pipe.extract(mockargv, pipe_file, table, [{"field_name": "col1", "data_type": "text", "params": []}, {"field_name": "col2", "data_type": "integer", "params": []}],
        db, source_conn_details, 1, None, mock_msg_queue)

    if raise_exception:
        mock_msg_queue.put.assert_called_with((mocker.ANY, mocker.ANY, const.ERROR, mocker.ANY))
    else:
        if directunload == const.BCP:
            mock_subprocess.Popen.assert_called_once_with(
                [const.BCP,
                 expected_sql.format(
                     append_metacols=append_metacols,
                     append_lsn_select=append_lsn_select),
                 'queryout',
                 mocker.ANY,
                 '-S',
                 'sourcehost,1234',
                 '-U',
                 'foo',
                 '-P',
                 'bar',
                 '-d',
                 'mydb',
                 '-c',
                 '-t{}'.format(mockargv.targetdelimiter)],
                stderr=mocker.ANY,
                stdout=mocker.ANY
            )

            mock_msg_queue.put.assert_called_with((mocker.ANY, mock_lsn, const.SUCCESS, mocker.ANY))
        else:
            
            if dbtype == const.FILE:
                expected_sql = "mytable"

            mock_db.execute_query.assert_called_once_with(
                expected_sql.format(
                    append_metacols=append_metacols,
                    append_lsn_select=append_lsn_select),
                1000,
                post_process_func=mocker.ANY
            )

            mock_msg_queue.put.assert_called_with(
                (mocker.ANY, mock_lsn, const.SUCCESS, mocker.ANY)
            )

            if mockargv.extractlsn:
                result_row.pop()

            with open(pipe_file, 'r') as f:
                for l in f:
                    print("result_row={}".format(result_row))
                    assert l == const.FIELD_DELIMITER.join(map(str, result_row)) + '\n'


def test_extract_data(data_extract_data, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    column_list = data_extract_data.input_column_list
    dbtype = data_extract_data.input_dbtype
    extractlsn = data_extract_data.input_extractlsn
    samplerows = data_extract_data.input_samplerows
    lock = data_extract_data.input_lock
    query_condition = data_extract_data.input_query_condition
    expected_sql = data_extract_data.expected_sql
    metacols= data_extract_data.input_metacols

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'lock': lock,
        'samplerows': samplerows,
        'extractlsn': extractlsn,
        'metacols': metacols,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_db = build_mock_db(mocker, mock_source_db_config, dbtype)
    db = build_initsync_db(mocker, mock_db, mockargv, logger)

    def mock_function(argv, table, extract_data_sql):
        pass

    db.extract_data(column_list, table, query_condition, mock_function)

    # Assert called with SQL str
    mock_db.execute_query.assert_called_once_with(
        expected_sql, const.DEFAULT_ARRAYSIZE,
        post_process_func=mocker.ANY)


def test_create_pipe(mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    file_path = initsync_pipe.create_pipe(mockargv, table, logger)

    assert file_path == os.path.join(mockargv.workdirectory,
                                     "{}.fifo".format(table.name))
    assert os.path.exists(file_path)


def test_get_output_filename(mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    filename = initsync_pipe.get_output_filename(mockargv, table)

    assert filename == os.path.join(mockargv.workdirectory,
        "{}_{}".format(table.name, unittest_utils.TEST_OUTFILE))


def test_get_raw_filename(mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)

    filename = initsync_pipe.get_raw_filename(mockargv, table)

    assert filename == os.path.join(mockargv.workdirectory,
        "{}_{}".format(table.name, unittest_utils.TEST_OUTFILE))


def test_report_initsync_summary(data_report_initsync_summary, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)
    mockpc = build_mock_pc(mocker)

    mock_send = mocker.patch("data_pipeline.initsync_pipe.mailer.send")

    assert not mock_send.called

    initsync_pipe.report_initsync_summary(
        mockargv, data_report_initsync_summary.input_all_table_results, mockpc,
        datetime.datetime.now(), datetime.datetime.now(), 1)

    mockpc.update.assert_called_once_with(
        total_count=data_report_initsync_summary.expected_total_count,
        min_lsn=data_report_initsync_summary.expected_min_lsn,
        max_lsn=data_report_initsync_summary.expected_max_lsn,
        comment="Completed InitSync",
        status=data_report_initsync_summary.expected_status,
        applier_marker=data_report_initsync_summary.expected_run_id)

    (args, kwargs) = mock_send.call_args_list[0]
    assert kwargs['plain_text_message'] is not None
    assert kwargs['html_text_message'] is not None

    mock_send.assert_called_once_with(
        mockargv.notifysender,
        data_report_initsync_summary.expected_mailing_list,
        data_report_initsync_summary.expected_subject,
        mockargv.notifysmtpserver,
        plain_text_message=mocker.ANY,
        html_text_message=mocker.ANY)


@pytest.mark.parametrize("prev_status, curr_status, expected", [
    (const.SUCCESS, const.ERROR, const.WARNING),
    (const.ERROR, const.SUCCESS, const.WARNING),
    (None, const.SUCCESS, const.SUCCESS),
    (None, const.ERROR, const.ERROR),
    (const.ERROR, const.ERROR, const.ERROR),
    (const.SUCCESS, const.SUCCESS, const.SUCCESS),
])
def test_combine_statuses(prev_status, curr_status, expected, mocker, setup):
    assert initsync_pipe.combine_statuses(prev_status, curr_status) == expected


@pytest.mark.parametrize("vacuum, analyze, expected_sql", [
    (True, False, "VACUUM FULL ctl.mytable"),
    (False, True, "ANALYZE ctl.mytable"),
])
def test_execute_post_processing(vacuum, analyze, expected_sql, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'vacuum': vacuum,
        'analyze': analyze,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_db = build_mock_db(mocker, mock_source_db_config, const.POSTGRES)
    db = build_initsync_db(mocker, mock_db, mockargv, logger)

    table_name = TableName("ctl", "mytable")
    initsync_pipe.execute_post_processing(mockargv, table_name, db)
    mock_db.execute.assert_called_with(expected_sql)


def test_report_no_active_schemas_tables(mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv = mocker.Mock(**mockargv_config)
    mockpc = build_mock_pc(mocker)

    initsync_pipe.report_no_active_schemas_tables(mockargv, mockpc, logger)
    mockpc.update.assert_called_with(comment=mocker.ANY, status=const.WARNING)


def test_execute_pre_processing(data_execute_pre_processing, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    loaddefinition = data_execute_pre_processing.loaddefinition
    datatypemap = data_execute_pre_processing.datatypemap
    source_dbtype = data_execute_pre_processing.source_dbtype
    target_dbtype = data_execute_pre_processing.target_dbtype
    column_list = data_execute_pre_processing.column_list
    pk_list = data_execute_pre_processing.pk_list
    query_condition = data_execute_pre_processing.query_condition
    delete = data_execute_pre_processing.delete
    truncate = data_execute_pre_processing.truncate
    droptable = data_execute_pre_processing.droptable
    droptablecascade = data_execute_pre_processing.droptablecascade
    createtable = data_execute_pre_processing.createtable
    expected_sql = data_execute_pre_processing.expected_sql

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'sourcedbtype': data_execute_pre_processing.source_dbtype,
        'targetdbtype': target_dbtype,
        'delete': data_execute_pre_processing.delete,
        'truncate': data_execute_pre_processing.truncate,
        'droptable': data_execute_pre_processing.droptable,
        'droptablecascade': data_execute_pre_processing.droptablecascade,
        'createtable': data_execute_pre_processing.createtable,
        'loaddefinition': data_execute_pre_processing.loaddefinition,
        'datatypemap': data_execute_pre_processing.datatypemap,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_db = build_mock_db(mocker,
                            mock_source_db_config,
                            data_execute_pre_processing.target_dbtype)

    target_db = build_initsync_db(mocker, mock_db, mockargv, logger)

    initsync_pipe.execute_pre_processing(
        mockargv, target_db, table, query_condition,
        column_list, pk_list, 1, logger
    )

    mock_db.assert_has_calls([
        mocker.call.execute(expected_sql_entry)
        for expected_sql_entry in expected_sql
    ])


@pytest.mark.parametrize("dbtype, nullstring, directload, raise_exception, mock_stdout_filename, truncate, delete, assumematchingcolumns", [
    (const.POSTGRES, const.NULL, None, False, None, False, False, False),
    (const.POSTGRES, const.NULL, None, False, None, False, False, True),
    (const.POSTGRES, "NA", None, False, None, False, False, False),
    (const.POSTGRES, const.NULL, const.GPLOAD, False, None, False, False, False),
    (const.POSTGRES, "NA", const.GPLOAD, False, None, False, False, False),

    (const.GREENPLUM, const.NULL, None, True, None, False, False, False),
    (const.GREENPLUM, "NA", None, True, None, False, False, False),
    (const.GREENPLUM, const.NULL, const.GPLOAD, True, "mock_gpload_output", False, False, False),
    (const.GREENPLUM, "NA", const.GPLOAD, True, "mock_gpload_output", False, False, False),

    (const.GREENPLUM, const.NULL, None, False, None, False, False, False),
    (const.GREENPLUM, "NA", None, False, None, False, False, False),
    (const.GREENPLUM, const.NULL, const.GPLOAD, False, "mock_gpload_output", False, False, False),
    (const.GREENPLUM, "NA", const.GPLOAD, False, "mock_gpload_output", False, False, False),
    (const.GREENPLUM, "NA", const.GPLOAD, False, "mock_gpload_output", False, False, True),
])
def test_apply(dbtype, nullstring, directload, raise_exception,
               mock_stdout_filename, truncate, delete,
               assumematchingcolumns, mocker, setup):

    (mockargv_config, mock_source_db_config, mock_target_db_config,
     table, logger, tmpdir) = setup

    (initsync_db,
     mock_db,
     mockargv,
     mock_msg_queue,
     pipe_file,
     mock_subprocess,
     mock_pc_detail) = setup_test_apply_mocks(mockargv_config,
                                   mock_source_db_config,
                                   dbtype,
                                   nullstring,
                                   directload,
                                   raise_exception,
                                   mock_stdout_filename,
                                   truncate,
                                   delete,
                                   assumematchingcolumns,
                                   logger,
                                   mocker)

    target_conn_details = dbuser.get_dbuser_properties(mockargv.targetuser)
    # Execute the function under test
    initsync_pipe.apply(
        mockargv, pipe_file, table,
        [{"field_name": "col1", "data_type": "text", "params": []}, {"field_name": "col2", "data_type": "integer", "params": []}],
        initsync_db, target_conn_details, 1, None, mock_msg_queue)

    # Check assertions
    if directload == const.GPLOAD and dbtype == const.GREENPLUM:

        if raise_exception:
            mock_msg_queue.put.assert_called_with((mocker.ANY, const.ERROR, mocker.ANY))
            mock_pc_detail.update.assert_called_once_with(
                comment=mocker.ANY,
                status=const.ERROR,
            )
            return

        # Check the generated gpload config and log files
        generated_gpload_config_file = os.path.join(
            mockargv.workdirectory,
            "{}_gpload_config.yaml".format(table.name),
        )
        generated_gpload_log_file = os.path.join(
            mockargv.workdirectory,
            "{}_gpload.out".format(table.name),
        )
        with open(generated_gpload_config_file, 'r') as gpload_file:
            gpload_config = yaml.load(gpload_file)
            assert_gpload_base_config(gpload_config)

            gpload_input_config = gpload_config['GPLOAD']['INPUT']
            assert_gpload_input_config(gpload_input_config, pipe_file, mockargv)

            gpload_output_config = gpload_config['GPLOAD']['OUTPUT']
            assert_gpload_output_config(gpload_output_config, table)

            gpload_preload_config = gpload_config['GPLOAD']['PRELOAD']
            assert_gpload_preload_config(gpload_preload_config, mockargv.truncate)

            gpload_sql_config = gpload_config['GPLOAD']['SQL']
            assert_gpload_sql_config(gpload_sql_config, mockargv.delete)

        # Check gpload is called correctly
        mock_subprocess.Popen.assert_called_once_with(
            [const.GPLOAD,
             '-f',
             generated_gpload_config_file,
             '--gpfdist_timeout', '300',
             '-l',
             generated_gpload_log_file,
             '-V',
             '-v',],
            stderr=mocker.ANY,
            stdout=mocker.ANY
        )

    else:
        if mockargv.assumematchingcolumns:
            expected_column_list = None
        else:
            expected_column_list = ['col1', 'col2', 'ctl_ins_ts', 'ctl_upd_ts']

        mock_db.copy_expert.assert_called_once_with(
            input_file=mocker.ANY,
            table_name=table,
            sep=mockargv.targetdelimiter,
            null_string=nullstring,
            column_list=expected_column_list,
            quote_char=mockargv.quotechar,
            escape_char=chr(const.ASCII_RECORDSEPARATOR),
            size=mockargv.buffersize,
            header=mockargv.header,
        )

    mock_msg_queue.put.assert_called_with((mocker.ANY, 'SUCCESS', mocker.ANY))

    mock_pc_detail.update.assert_called_once_with(
        comment=mocker.ANY,
        status=const.SUCCESS,
        source_row_count=99,
        insert_row_count=99,
    )


def assert_gpload_base_config(gpload_config):
    assert gpload_config['VERSION'] == "1.0.0.1"
    assert gpload_config['DATABASE'] == "mydb"
    assert gpload_config['USER'] == "foo"
    assert gpload_config['HOST'] == "targethost"
    assert gpload_config['PORT'] == 1234


def assert_gpload_input_config(gpload_input_config, expected_input_filename,
                               mockargv):
    for inputattr in gpload_input_config:
        k = inputattr.keys()[0]
        v = inputattr[k]
        if k == "SOURCE":
            assert_gpload_source_config(v, expected_input_filename, mockargv)
        elif k == "FORMAT":
            assert v == "TEXT"
        elif k == "DELIMITER":
            assert v == mockargv.targetdelimiter
        elif k == "ESCAPE":
            assert v == "OFF"
        elif k == "NULL_AS":
            assert v == mockargv.nullstring
        elif k == "QUOTE":
            assert v == mockargv.quotechar
        elif k == "HEADER":
            assert v == mockargv.header
        elif k == "ENCODING":
            assert v == mockargv.clientencoding
        elif k == "COLUMNS":
            if mockargv.assumematchingcolumns:
                assert v is None
            else:
                assert v == [
                    {'col1': 'text'},
                    {'col2': 'integer'},
                    {'ctl_ins_ts': 'timestamp'},
                    {'ctl_upd_ts': 'timestamp'},
                ]


def assert_gpload_source_config(source_value, expected_input_file, mockargv):
    for key, value in source_value.items():
        if key == "LOCAL_HOSTNAME":
            assert len(value) == 1
            assert value[0] == mockargv.localhost
        elif key == "PORT_RANGE":
            assert len(value) == 2
            assert value == mockargv.portrange
        elif key == "FILE":
            assert len(value) == 1
            assert value[0] == expected_input_file


def assert_gpload_output_config(gpload_output_config, table):
    for inputattr in gpload_output_config:
        k = inputattr.keys()[0]
        v = inputattr[k]
        if k == "TABLE":
            assert v == table.fullname
        if k == "MODE":
            assert v == "insert"


def assert_gpload_preload_config(gpload_preload_config, truncate):
    for inputattr in gpload_preload_config:
        k = inputattr.keys()[0]
        v = inputattr[k]
        if k == "TRUNCATE":
            assert v == truncate
        if k == "REUSE_TABLES":
            assert v == False


def assert_gpload_sql_config(gpload_sql_config, delete):
    delete_sql = "" if delete else None
    for inputattr in gpload_sql_config:
        k = inputattr.keys()[0]
        v = inputattr[k]
        if k == "BEFORE":
            assert v == delete_sql
        if k == "AFTER":
            assert v == None


def setup_test_apply_mocks(mockargv_config, mock_source_db_config,
                dbtype, nullstring, directload, raise_exception,
                mock_stdout_filename, truncate, delete,
                assumematchingcolumns, logger, mocker):
    # Setup the mocks
    mockargv_config = utils.merge_dicts(mockargv_config, {
        "nullstring": nullstring,
        "directload": directload,
        # yaml reader doesn't like special characters
        "sourcedelimiter": const.COMMA,
        "targetdelimiter": const.PIPE,
        "delimiter": const.COMMA,
        "localhost": "mylocalhostname",
        "portrange": [5001, 5005],
        "header": True,
        "truncate": truncate,
        "delete": delete,
        "assumematchingcolumns": assumematchingcolumns,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_source_db_config = utils.merge_dicts(mock_source_db_config, {
        "copy_expert.return_value": 99,
    })

    mock_db = build_mock_db(mocker, mock_source_db_config, dbtype)
    initsync_db = build_initsync_db(mocker, mock_db, mockargv, logger)

    mock_msg_queue_config = {"put.return_value": None}
    mock_msg_queue = mocker.Mock(**mock_msg_queue_config)

    pipe_file = os.path.join(mockargv.workdirectory, "myfakefifo")
    open(pipe_file, 'a').close()

    mock_pc_detail_config = {"update.return_value": None}
    mock_pc_detail = mocker.Mock(**mock_pc_detail_config)
    mock_pc_detail_cons = mocker.patch(
        "data_pipeline.initsync_pipe.ProcessControlDetail")
    mock_pc_detail_cons.return_value = mock_pc_detail

    out = const.EMPTY_STRING
    if mock_stdout_filename:
        currdir = os.path.dirname(os.path.abspath(__file__))
        mock_stdout_filepath = os.path.join(currdir, mock_stdout_filename)
        with open(mock_stdout_filepath) as f:
            out = f.read()
    err = "sample error output"

    mock_cmd_return_config = {
        "communicate.return_value": (out, err),
        "returncode": 1 if raise_exception else 0,
    }

    mock_cmd_return = mocker.Mock(**mock_cmd_return_config)
    mock_subprocess = mocker.patch(
        "data_pipeline.initsync.greenplumdb.subprocess")
    mock_subprocess.Popen.return_value = mock_cmd_return

    return (initsync_db, mock_db, mockargv, mock_msg_queue, pipe_file, mock_subprocess, mock_pc_detail)


@pytest.mark.parametrize("ssps, streamhost, streamchannel, seektoend_called_times", [
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        "myhost", "mytopic", 1),
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        "myhost", None, 0),
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        None, "mytopic", 0),
    ([("mysourceschema", "mytable", "mytargetschema", "col1 like '%foo%'")],
        None, None, 0),
    (None, None, None, 0),
    ([], None, None, 0),
])
def test_main(ssps, streamhost, streamchannel, seektoend_called_times, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        'streamhost': streamhost,
        'streamchannel': streamchannel,
    })
    mockargv = mocker.Mock(**mockargv_config)

    def seek_to_end_se(timeout):
        print("kafka_consumer: seek_to_end called with timeout = {}"
              .format(timeout))
    mock_kafka_consumer_config = { "seek_to_end.side_effect": seek_to_end_se }
    mock_kafka_consumer = mocker.Mock(**mock_kafka_consumer_config)

    mock_get_program_args = mocker.patch(
        "data_pipeline.initsync_pipe.get_program_args")
    mock_get_program_args.return_value = mockargv

    mock_kafka_consumer_cons = mocker.patch(
        "data_pipeline.stream.factory.KafkaConsumer")
    mock_kafka_consumer_cons.return_value = mock_kafka_consumer

    mock_get_ssp = mocker.patch(
        "data_pipeline.initsync_pipe.get_source_system_profile_params")
    mock_get_ssp.return_value = ssps

    mock_parallelise_initsync = mocker.patch(
        "data_pipeline.initsync_pipe.parallelise_initsync")
    mock_parallelise_initsync.return_value = {
        table: ("LSN0", const.SUCCESS, "CCPI", "All good")
    }

    def report_error_se(message, process_control, logger):
        print("An error occurred in test: {}".format(message))
    mock_report_error = mocker.patch("data_pipeline.initsync_pipe.report_error")
    mock_report_error.size_effect = report_error_se

    mock_send = mocker.patch("data_pipeline.initsync_pipe.mailer.send")

    initsync_pipe.main()

    assert mock_kafka_consumer.seek_to_end.call_count == seektoend_called_times

    mock_report_error.assert_not_called()
    mock_send.assert_called_once()


@pytest.mark.parametrize("input_build_mock_pc, input_alert", [
    (True, True),
    (False, True),
    (True, False),
    (False, False),
])
def test_report_error(input_build_mock_pc, input_alert, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    mock_pc = None
    if input_build_mock_pc:
        mock_pc = build_mock_pc(mocker)

    mockargv = mocker.Mock(**mockargv_config)

    mock_send = mocker.patch("data_pipeline.initsync_pipe.mailer.send")

    initsync_pipe.report_error(mockargv, "an error", mock_pc,
                               logger, alert=input_alert)

    if input_alert:
        mock_send.assert_called_once_with(
            mockargv.notifysender,
            set(['someone@gmail.com', 'someone@error.com']),
            'myprofile InitSync ERROR',
            mockargv.notifysmtpserver,
            plain_text_message="an error"
        )

    if input_build_mock_pc:
        mock_pc.update.assert_called_once_with(
            comment="an error",
            status=const.ERROR
        )


@pytest.mark.parametrize("tablelist, isfile, expected_ssp_params", [
    ([], False, []),
    (["tableA"], False, [
        ("sys", "tableA", "ctl", None)]),
    (["tableA", "tableB"], False, [
        ("sys", "tableA", "ctl", None),
        ("sys", "tableB", "ctl", None)]),
    (["test.tabs"], True, [
        ("sys", "tableA", "ctl", None),
        ("sys", "tableB", "ctl", None),
        ("sys", "tableC", "ctl", None)]),
])
def test_get_source_system_profile_params_without_auditdb(
        tablelist, isfile, expected_ssp_params, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    # Create a mock tablelist file
    if isfile:
        filename = str(tmpdir.mkdir("ssp").join(tablelist[0]))
        with open(filename, 'a') as f:
            f.write("tableA\n")
            f.write("tableB\n")
            f.write("tableC\n")

        tablelist = [filename]

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'auditschema': None,
        'audituser': None,
        'tablelist': tablelist,
    })
    mockargv = mocker.Mock(**mockargv_config)

    ssp_params = initsync_pipe.get_source_system_profile_params(mockargv)
    assert ssp_params == expected_ssp_params


@pytest.mark.parametrize("nullstring, record, expected_payload", [
    (const.NULL, ('a', None, 99), "a{d}NULL\n".format(d=const.FIELD_DELIMITER)),
    ("NA", ('a', None, 99), "a{d}NA\n".format(d=const.FIELD_DELIMITER)),
    (const.EMPTY_STRING, ('a', None, 99), "a{d}\n".format(d=const.FIELD_DELIMITER)),
    (const.EMPTY_STRING, ('a', None, None, 99), "a{d}{d}\n".format(d=const.FIELD_DELIMITER)),
])
def test_write(nullstring, record, expected_payload, mocker, setup):
    (mockargv_config, mock_source_db_config, mock_target_db_config, table, logger, tmpdir) = setup

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'nullstring': nullstring,
    })
    mockargv = mocker.Mock(**mockargv_config)

    mock_fifo_file = mocker.Mock(**{"write.return_value": None})

    lsn = initsync_pipe.write(mockargv, record, mock_fifo_file, None)

    mock_fifo_file.write.assert_called_once_with(expected_payload)
    assert lsn == 99
