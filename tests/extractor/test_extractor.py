import pytest
import data_pipeline.constants.const as const
import data_pipeline.db.factory as db_factory
import data_pipeline.extractor.factory as extractor_factory
import tests.unittest_utils as unittest_utils

from data_pipeline.audit.factory import AuditFactory
from pytest_mock import mocker


@pytest.fixture
def setup(mocker, tmpdir):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)
    yield(mockargv)


@pytest.mark.parametrize("dbtype, raise_exception", [
    (const.ORACLE, False),
    (const.ORACLE, True),
    (const.MSSQL, False),
    (const.MSSQL, True),
])
def test_extract_report_error(dbtype, raise_exception, mocker, setup):
    (mockargv) = setup
    
    # Mock out stuff we don't care about for now
    db = db_factory.build(dbtype)
    audit_factory = mocker.Mock()

    # At this point, we don't care about these functions in the extractor
    mocker.patch("data_pipeline.extractor.extractor.Extractor._init_stream_output")
    mocker.patch("data_pipeline.extractor.extractor.Extractor._ensure_schemas_and_tables_are_set")
    mocker.patch("data_pipeline.extractor.cdc_extractor.CdcExtractor._extract_source_data")
    mock_report_error = mocker.patch("data_pipeline.extractor.extractor.Extractor.report_error")

    if dbtype == const.ORACLE:
        module_suffix = "oracledb.cx_Oracle"
    else:
        module_suffix = "mssqldb.pymssql"

    mock_connect = mocker.patch("data_pipeline.db.{}.connect".format(module_suffix))

    if raise_exception:
        def exception_raiser(*args, **kwargs):
            raise Exception("Connection failed!")
        mock_connect.side_effect = exception_raiser

    extractor = extractor_factory.build(const.CDCEXTRACT, db, mockargv, audit_factory)
    extractor.extract()

    mock_connect.assert_called_once()

    if raise_exception:
        mock_report_error.assert_called_once()
    else:
        mock_report_error.assert_not_called()
