import pytest
import logging

import data_pipeline.constants.const as const
import tests.unittest_utils as unittest_utils
import data_pipeline.utils.utils as utils
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.db.filedb as db_filedb
import data_pipeline.initsync.filedb as initsync_filedb

from data_pipeline.sql.table_name import TableName


@pytest.fixture
def setup_base(tmpdir, mocker):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)

    unittest_utils.setup_logging(mockargv.workdirectory)

    logger = logging.getLogger(__name__)
    
    mockdb_config = {
        'close.return_value': None,
    }
    yield (mockargv_config, logger, mockdb_config, tmpdir)


@pytest.fixture
def setup(tmpdir, mocker, setup_base):
    (mockargv_config, logger, mockdb_config, tmpdir) = setup_base

    dirpath = tmpdir.mkdir("test")
    filepath = dirpath.join("sample.csv")
    filepath.open("a").close()

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'sourceuser': str(dirpath)
    })
    mockargv = mocker.Mock(**mockargv_config)

    db = db_filedb.FileDb()
    connection_details = dbuser.get_dbuser_properties(mockargv.sourceuser)
    db.connect(connection_details)

    filedb = initsync_filedb.FileDb(mockargv, db, logger, mocker.ANY, mocker.ANY)

    yield(filedb)


@pytest.mark.parametrize("create_file", [
    (True),
    (False),
])
def test_table_exists(create_file, setup_base, mocker):
    (mockargv_config, logger, mockdb_config, tmpdir) = setup_base

    dirpath = tmpdir.mkdir("test_table_exists")
    filepath = dirpath.join("sample.csv")
    if create_file:
        filepath.open("a").close()

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'sourceuser': str(dirpath)
    })
    mockargv = mocker.Mock(**mockargv_config)

    db = db_filedb.FileDb()
    connection_details = dbuser.get_dbuser_properties(mockargv.sourceuser)
    db.connect(connection_details)

    filedb = initsync_filedb.FileDb(mockargv, db, logger, mocker.ANY, mocker.ANY)

    table = TableName("myschema", "sample")
    exists = filedb.table_exists(table)
    assert exists if create_file else not exists


@pytest.mark.parametrize("delimiter, quotechar", [
    (const.COMMA, const.DOUBLE_QUOTE),
    (const.PIPE, const.SINGLE_QUOTE),
])
def test_filedb_attributes_set(delimiter, quotechar, setup_base, mocker):
    (mockargv_config, logger, mockdb_config, tmpdir) = setup_base

    mockargv_config = utils.merge_dicts(mockargv_config, {
        'delimiter': delimiter,
        'quotechar': quotechar,
    })
    mockargv = mocker.Mock(**mockargv_config)

    db = db_filedb.FileDb()
    connection_details = dbuser.get_dbuser_properties(mockargv.sourceuser)
    db.connect(connection_details)

    filedb = initsync_filedb.FileDb(mockargv, db, logger, delimiter, quotechar)

    assert (db.delimiter == delimiter and
            db.quotechar == quotechar)


def test_get_decorated_source_column_list(setup, mocker):
    (filedb) = setup
    column_list = filedb.get_decorated_source_column_list([("col1", "datatype", [])], const.TARGET)

    assert column_list == [{"field_name": "col1", "data_type": "datatype", "params": []}]


def test_get_decorated_target_column_list(setup, mocker):
    (filedb) = setup
    column_list = filedb.get_decorated_target_column_list([("col1", "datatype", [])], const.TARGET)

    assert column_list == [{"field_name": "col1", "data_type": "datatype", "params": []}]
