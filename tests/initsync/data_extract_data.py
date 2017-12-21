import collections
import data_pipeline.constants.const as const


TestCase = collections.namedtuple('TestCase', "input_dbtype input_column_list, input_extractlsn, input_samplerows, input_lock, input_query_condition, input_metacols, expected_sql")


# MSSQL Extract Data SQL

MSSQL_EXTRACT_LSN_COLS1_SQL = """
        SELECT 
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN
        FROM myschema.mytable
        
        WHERE 1=1"""


MSSQL_EXTRACT_LSN_COLS1_WITH_NOLOCK_SQL = """
        SELECT 
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1"""


MSSQL_EXTRACT_LSN_COLS1_TOP5_SQL = """
        SELECT TOP 5
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN
        FROM myschema.mytable
        
        WHERE 1=1"""


MSSQL_EXTRACT_LSN_COLS1_WITH_NOLOCK_TOP5_SQL = """
        SELECT TOP 5
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1"""


MSSQL_EXTRACT_LSN_COLS1_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL = """
        SELECT TOP 5
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1
          AND col1 like '%foo%'"""


MSSQL_EXTRACT_LSN_COLS3_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL = """
        SELECT TOP 5
              col1
            , col2
            , col3
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1
          AND col1 like '%foo%'"""


MSSQL_EXTRACT_COLS1_SQL = """
        SELECT 
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        
        WHERE 1=1"""


MSSQL_EXTRACT_COLS1_WITH_NOLOCK_SQL = """
        SELECT 
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1"""


MSSQL_EXTRACT_COLS1_TOP5_SQL = """
        SELECT TOP 5
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        
        WHERE 1=1"""


MSSQL_EXTRACT_COLS1_WITH_NOLOCK_TOP5_SQL = """
        SELECT TOP 5
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1"""


MSSQL_EXTRACT_COLS1_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL = """
        SELECT TOP 5
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1
          AND col1 like '%foo%'"""


MSSQL_EXTRACT_COLS3_WITH_NOLOCK_TOP5_QUERY_TOKEN_CONDITION_SQL = """
        SELECT TOP 5
              col1
            , col2
            , col3
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1
          AND col1 > (select (MAX(col1) - 10) from myschema.mytable)"""



MSSQL_EXTRACT_COLS3_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL = """
        SELECT TOP 5
              col1
            , col2
            , col3
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WITH (NOLOCK)
        WHERE 1=1
          AND col1 like '%foo%'"""


# Oracle Extract Data SQL

ORACLE_EXTRACT_LSN_COLS1_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER
               FROM DUAL) AS SYS_SOURCE_TRANSACTION_ID
        FROM myschema.mytable
        WHERE 1=1"""


ORACLE_EXTRACT_LSN_COLS1_TOP5_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER
               FROM DUAL) AS SYS_SOURCE_TRANSACTION_ID
        FROM myschema.mytable
        WHERE 1=1
          AND ROWNUM <= 5"""


ORACLE_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , (SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER
               FROM DUAL) AS SYS_SOURCE_TRANSACTION_ID
        FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'
          AND ROWNUM <= 5"""


ORACLE_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_NO_METACOLS_SQL = """
        SELECT
              col1
            , (SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER
               FROM DUAL) AS SYS_SOURCE_TRANSACTION_ID
        FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'
          AND ROWNUM <= 5"""


ORACLE_EXTRACT_COLS1_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1"""


ORACLE_EXTRACT_COLS1_TOP5_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1
          AND ROWNUM <= 5"""


ORACLE_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'
          AND ROWNUM <= 5"""


ORACLE_EXTRACT_COLS1_TOP5_QUERY_CONDITION_NO_METACOLS_SQL = """
        SELECT
              col1
        FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'
          AND ROWNUM <= 5"""


ORACLE_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_NO_METACOLS_SQL = """
        SELECT
              col1
        FROM myschema.mytable
        WHERE 1=1
          AND col1 > (select (MAX(col1) - 10) from myschema.mytable)
          AND ROWNUM <= 5"""



# Postgres Extract Data SQL

POSTGRES_EXTRACT_LSN_COLS1_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , 'UNSUPPORTED BY DB'
        FROM myschema.mytable
        WHERE 1=1"""


POSTGRES_EXTRACT_LSN_COLS1_TOP5_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , 'UNSUPPORTED BY DB'
        FROM myschema.mytable
        WHERE 1=1
        LIMIT 5"""


POSTGRES_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
            , 'UNSUPPORTED BY DB'
        FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'
        LIMIT 5"""


POSTGRES_EXTRACT_COLS1_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1"""


POSTGRES_EXTRACT_COLS1_TOP5_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1
        LIMIT 5"""


POSTGRES_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'
        LIMIT 5"""


POSTGRES_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_SQL = """
        SELECT
              col1
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp
        FROM myschema.mytable
        WHERE 1=1
          AND col1 > (select (MAX(col1) - 10) from myschema.mytable)
        LIMIT 5"""


# Greenplum Extract Data SQL

GREENPLUM_EXTRACT_LSN_COLS1_SQL = POSTGRES_EXTRACT_LSN_COLS1_SQL

GREENPLUM_EXTRACT_LSN_COLS1_TOP5_SQL = POSTGRES_EXTRACT_LSN_COLS1_TOP5_SQL

GREENPLUM_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL = POSTGRES_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL

GREENPLUM_EXTRACT_COLS1_SQL = POSTGRES_EXTRACT_COLS1_SQL

GREENPLUM_EXTRACT_COLS1_TOP5_SQL = POSTGRES_EXTRACT_COLS1_TOP5_SQL

GREENPLUM_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL = POSTGRES_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL

GREENPLUM_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_SQL = POSTGRES_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_SQL

DEFAULT_METACOLS = {
    const.METADATA_INSERT_TS_COL: 'ctl_ins_ts',
    const.METADATA_UPDATE_TS_COL: 'ctl_upd_ts'
}

tests=[

    # MSSQL
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=None,
        input_lock=True,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_LSN_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=None,
        input_lock=False,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_LSN_COLS1_WITH_NOLOCK_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=True,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_LSN_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_LSN_COLS1_WITH_NOLOCK_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False,
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_LSN_COLS1_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}, {'field_name': 'col2', 'data_type': 'text', 'params': []}, {'field_name': 'col3', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False,
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_LSN_COLS3_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=None,
        input_lock=True,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS1_SQL
    ),

    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=None,
        input_lock=False,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS1_WITH_NOLOCK_SQL
    ),

    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=True,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS1_TOP5_SQL
    ),

    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False,
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS1_WITH_NOLOCK_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False,
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS1_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}, {'field_name': 'col2', 'data_type': 'text', 'params': []}, {'field_name': 'col3', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'", 
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS3_WITH_NOLOCK_TOP5_QUERY_CONDITION_SQL
    ),

    TestCase(
        input_dbtype=const.MSSQL,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}, {'field_name': 'col2', 'data_type': 'text', 'params': []}, {'field_name': 'col3', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 > (select (MAX(col1) - 10) from {SCHEMA}.{TABLE})", 
        input_metacols=DEFAULT_METACOLS,
        expected_sql=MSSQL_EXTRACT_COLS3_WITH_NOLOCK_TOP5_QUERY_TOKEN_CONDITION_SQL
    ),

    # Oracle
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=None,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=ORACLE_EXTRACT_LSN_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=ORACLE_EXTRACT_LSN_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=ORACLE_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols={},
        expected_sql=ORACLE_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_NO_METACOLS_SQL
    ),
    
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=None,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=ORACLE_EXTRACT_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=ORACLE_EXTRACT_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=ORACLE_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols={},
        expected_sql=ORACLE_EXTRACT_COLS1_TOP5_QUERY_CONDITION_NO_METACOLS_SQL
    ),
    TestCase(
        input_dbtype=const.ORACLE,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 > (select (MAX(col1) - 10) from {SCHEMA}.{TABLE})",
        input_metacols={},
        expected_sql=ORACLE_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_NO_METACOLS_SQL
    ),


    # Postgres
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=None,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_LSN_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_LSN_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=None,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.POSTGRES,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 > (select (MAX(col1) - 10) from {SCHEMA}.{TABLE})",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=POSTGRES_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_SQL
    ),

    # Greenplum
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=None,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_LSN_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_LSN_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=True,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_LSN_COLS1_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=None,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_COLS1_SQL
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition=None,
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_COLS1_TOP5_SQL
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 like '%foo%'",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_COLS1_TOP5_QUERY_CONDITION_SQL
    ),
    TestCase(
        input_dbtype=const.GREENPLUM,
        input_column_list=[{'field_name': 'col1', 'data_type': 'text', 'params': []}],
        input_extractlsn=False,
        input_samplerows=5,
        input_lock=False, 
        input_query_condition="col1 > (select (MAX(col1) - 10) from {SCHEMA}.{TABLE})",
        input_metacols=DEFAULT_METACOLS,
        expected_sql=GREENPLUM_EXTRACT_COLS1_TOP5_QUERY_TOKEN_CONDITION_SQL
    ),
]
