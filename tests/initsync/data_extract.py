import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple("TestCase", "dbtype directunload extractlsn metacols raise_exception lsn_select expected_sql")


EXPECTED_ORACLE_QUERY = """
        SELECT
              col1
            , col2{append_metacols}{append_lsn_select}
        FROM myschema.mytable
        WHERE 1=1
          AND ROWNUM <= 10"""


EXPECTED_MSSQL_QUERY = """
        SELECT TOP 10
              col1
            , col2{append_metacols}{append_lsn_select}
        FROM myschema.mytable
        
        WHERE 1=1"""


EXPECTED_POSTGRES_QUERY = """
        SELECT
              col1
            , col2{append_metacols}{append_lsn_select}
        FROM myschema.mytable
        WHERE 1=1
        LIMIT 10"""


EXPECTED_FILE_QUERY = "mytable"


DEFAULT_METACOLS = {
    const.METADATA_INSERT_TS_COL: "ctl_ins_ts",
    const.METADATA_UPDATE_TS_COL: "ctl_upd_ts"
}


tests = [
    # Mssql
    TestCase(
        dbtype=const.MSSQL,
        directunload=const.BCP,
        extractlsn=False,
        metacols=None,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_MSSQL_QUERY,
    ),

    TestCase(
        dbtype=const.MSSQL,
        directunload=const.BCP,
        extractlsn=False,
        metacols=None,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_MSSQL_QUERY,
    ),

    TestCase(
        dbtype=const.MSSQL,
        directunload=const.BCP,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_MSSQL_QUERY,
    ),

    TestCase(
        dbtype=const.MSSQL,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_MSSQL_QUERY,
    ),

    TestCase(
        dbtype=const.MSSQL,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_MSSQL_QUERY,
    ),

    TestCase(
        dbtype=const.MSSQL,
        directunload=None,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_MSSQL_QUERY,
    ),


    # Oracle
    TestCase(
        dbtype=const.ORACLE,
        directunload=const.BCP,
        extractlsn=False,
        metacols=None,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_ORACLE_QUERY,
    ),

    TestCase(
        dbtype=const.ORACLE,
        directunload=const.BCP,
        extractlsn=False,
        metacols=None,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_ORACLE_QUERY,
    ),

    TestCase(
        dbtype=const.ORACLE,
        directunload=const.BCP,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_ORACLE_QUERY,
    ),

    TestCase(
        dbtype=const.ORACLE,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_ORACLE_QUERY,
    ),

    TestCase(
        dbtype=const.ORACLE,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_ORACLE_QUERY,
    ),

    TestCase(
        dbtype=const.ORACLE,
        directunload=None,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_ORACLE_QUERY,
    ),


    # Postgres
    TestCase(
        dbtype=const.POSTGRES,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_POSTGRES_QUERY,
    ),

    TestCase(
        dbtype=const.POSTGRES,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_POSTGRES_QUERY,
    ),

    TestCase(
        dbtype=const.POSTGRES,
        directunload=None,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_POSTGRES_QUERY,
    ),

    TestCase(
        dbtype=const.POSTGRES,
        directunload=None,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_POSTGRES_QUERY,
    ),


    # File
    TestCase(
        dbtype=const.FILE,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_FILE_QUERY,
    ),

    TestCase(
        dbtype=const.FILE,
        directunload=None,
        extractlsn=False,
        metacols=None,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_FILE_QUERY,
    ),

    TestCase(
        dbtype=const.FILE,
        directunload=None,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=False,
        lsn_select=None,
        expected_sql=EXPECTED_FILE_QUERY,
    ),

    TestCase(
        dbtype=const.FILE,
        directunload=None,
        extractlsn=False,
        metacols=DEFAULT_METACOLS,
        raise_exception=True,
        lsn_select=None,
        expected_sql=EXPECTED_FILE_QUERY,
    ),
]
