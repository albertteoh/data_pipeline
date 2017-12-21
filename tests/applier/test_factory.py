import pytest
import data_pipeline.applier.factory as applier_factory
import data_pipeline.db.factory as db_factory
import data_pipeline.constants.const as const
import tests.unittest_utils as unittest_utils

from data_pipeline.db.exceptions import UnsupportedDbTypeError
from data_pipeline.applier.applier import Applier


@pytest.fixture()
def setup(tmpdir, mocker):
    p = tmpdir.mkdir("test_apply_factory")

    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    mockargv = mocker.Mock(**mockargv_config)

    pc_config = {'insert.return_value': None, 'update.return_value': None}
    mock_pc = mocker.Mock(**pc_config)

    af_config = {'build_process_control.return_value': mock_pc}
    mock_audit_factory = mocker.Mock(**af_config)

    unittest_utils.mock_get_inactive_applied_tables(mocker, [])

    f = mocker.patch.object(Applier, '_get_max_lsn_source_system_profile')
    f = mocker.patch.object(Applier, '_get_last_apply_record')

    yield (mockargv, mock_audit_factory)


def test_build_postgres_applier(setup):
    (mockargv, mock_audit_factory) = setup
    db = db_factory.build(const.POSTGRES)
    applier = applier_factory.build(None, db, mockargv, mock_audit_factory)
    assert type(applier).__name__ == 'PostgresCdcApplier'


def test_build_greenplum_applier(setup):
    (mockargv, mock_audit_factory) = setup
    db = db_factory.build(const.GREENPLUM)
    applier = applier_factory.build(None, db, mockargv, mock_audit_factory)
    assert type(applier).__name__ == 'GreenplumCdcApplier'


def test_build_unsupported(setup):
    (mockargv, mock_audit_factory) = setup
    with pytest.raises(ImportError):
        db = db_factory.build("AnUnsupportedDatabase")
        db = applier_factory.build(None, db, mockargv, mock_audit_factory)
