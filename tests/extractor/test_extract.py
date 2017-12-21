import pytest

import data_pipeline.constants.const as const
import data_pipeline.extract as extract
import data_pipeline.utils.utils as utils
import tests.unittest_utils as unittest_utils


@pytest.fixture
def setup(mocker, tmpdir):
    mockargv_config = unittest_utils.get_default_argv_config(tmpdir)
    yield (mockargv_config)


@pytest.mark.parametrize("dbtype", [
    (const.ORACLE),
    (const.MSSQL),
    (const.POSTGRES),
    (const.GREENPLUM),
])
def test_get_source_db(dbtype, mocker, setup):
    (mockargv_config) = setup
    mockargv_config = utils.merge_dicts(mockargv_config, {
        "sourcedbtype": dbtype
    })
    mockargv = mocker.Mock(**mockargv_config)

    db = extract.get_source_db(mockargv)
    assert type(db).__name__.lower() == "{}db".format(dbtype.lower())
