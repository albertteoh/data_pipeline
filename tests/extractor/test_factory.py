import pytest
import data_pipeline.db.factory as db_factory
import data_pipeline.extractor.factory as extractor_factory
import tests.unittest_utils as utils
import data_pipeline.constants.const as const

from pytest_mock import mocker
from data_pipeline.db.exceptions import UnsupportedDbTypeError

@pytest.fixture()
def setup(tmpdir, mocker):
    mockargv_config = utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)

    pc_config = {'insert.return_value': None, 'update.return_value': None}
    mock_pc = mocker.Mock(**pc_config)

    af_config = {'build_process_control.return_value': mock_pc}
    mock_audit_factory = mocker.Mock(**af_config)
    utils.mock_build_kafka_producer(mocker)

    yield (mockargv, mock_audit_factory)


@pytest.mark.parametrize("dbtype, expect_class", [
    (const.ORACLE, "OracleCdcExtractor"),
    (const.MSSQL, "MssqlCdcExtractor"),
])
def test_build(dbtype, expect_class, setup):
    (mockargv, mock_audit_factory) = setup
    mode = const.CDCEXTRACT
    db = db_factory.build(dbtype)
    extractor = extractor_factory.build(mode, db, mockargv, mock_audit_factory)
    assert type(extractor).__name__ == expect_class


def test_build_unsupported(setup):
    (mockargv, mock_audit_factory) = setup
    with pytest.raises(ImportError):
        db = db_factory.build("AnUnsupportedDatabase")
        extractor = extractor_factory.build(db, mockargv, mock_audit_factory)
