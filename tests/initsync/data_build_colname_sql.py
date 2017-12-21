import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "description input_source_dbtype input_target_dbtype input_ignored_columns input_nonstandardcolumnnames input_definition_origin expected_sql_calls_on_definition_db expected_source_cols expected_target_cols")

# Oracle Column Name SQL

ORACLE_SOURCE_DEF_COLNAME_SQL = """
        SELECT column_name, data_type, data_length, data_precision, data_scale
        FROM ALL_TAB_COLUMNS
        WHERE owner      = UPPER('myschema')
          AND table_name = UPPER('mytable')
          AND data_type NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
          
        ORDER BY column_id"""


ORACLE_TARGET_DEF_COLNAME_SQL = """
        SELECT LOWER(column_name) AS column_name, data_type, data_length, data_precision, data_scale
        FROM ALL_TAB_COLUMNS
        WHERE owner      = UPPER('myschema')
          AND table_name = UPPER('mytable')
          AND data_type NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
          
        ORDER BY column_id"""


ORACLE_SOURCE_DEF_IGNORED_COLNAME_SQL = """
        SELECT column_name, data_type, data_length, data_precision, data_scale
        FROM ALL_TAB_COLUMNS
        WHERE owner      = UPPER('myschema')
          AND table_name = UPPER('mytable')
          AND data_type NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
          AND column_name NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY column_id"""


ORACLE_TARGET_DEF_IGNORED_COLNAME_SQL = """
        SELECT LOWER(column_name) AS column_name, data_type, data_length, data_precision, data_scale
        FROM ALL_TAB_COLUMNS
        WHERE owner      = UPPER('myschema')
          AND table_name = UPPER('mytable')
          AND data_type NOT IN ('SDO_GEOMETRY', 'RAW', 'BLOB')
          AND column_name NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY column_id"""

# MSSQL Column Name SQL

MSSQL_SOURCE_DEF_COLNAME_SQL = """
        SELECT [column_name], [data_type], [character_maximum_length], [numeric_precision], [numeric_scale]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('myschema')
          AND LOWER([table_name]) = LOWER('mytable')
          AND UPPER([data_type]) NOT IN ('IMAGE', 'VARBINARY')
          
        ORDER BY ordinal_position"""


MSSQL_TARGET_DEF_COLNAME_SQL = """
        SELECT LOWER([column_name]) AS [column_name], [data_type], [character_maximum_length], [numeric_precision], [numeric_scale]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('myschema')
          AND LOWER([table_name]) = LOWER('mytable')
          AND UPPER([data_type]) NOT IN ('IMAGE', 'VARBINARY')
          
        ORDER BY ordinal_position"""


MSSQL_SOURCE_DEF_IGNORED_COLNAME_SQL = """
        SELECT [column_name], [data_type], [character_maximum_length], [numeric_precision], [numeric_scale]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('myschema')
          AND LOWER([table_name]) = LOWER('mytable')
          AND UPPER([data_type]) NOT IN ('IMAGE', 'VARBINARY')
          AND [column_name] NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY ordinal_position"""


MSSQL_TARGET_DEF_IGNORED_COLNAME_SQL = """
        SELECT LOWER([column_name]) AS [column_name], [data_type], [character_maximum_length], [numeric_precision], [numeric_scale]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('myschema')
          AND LOWER([table_name]) = LOWER('mytable')
          AND UPPER([data_type]) NOT IN ('IMAGE', 'VARBINARY')
          AND [column_name] NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY ordinal_position"""

# Postgres Column Name SQL

POSTGRES_SOURCE_DEF_COLNAME_SQL = """
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          
        ORDER BY ordinal_position"""


POSTGRES_TARGET_DEF_COLNAME_SQL = """
        SELECT LOWER(column_name) AS column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          
        ORDER BY ordinal_position"""


POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL = """
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          AND column_name NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY ordinal_position"""


POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL = """
        SELECT LOWER(column_name) AS column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          AND column_name NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY ordinal_position"""


# Greenplum Column Name SQL

GREENPLUM_SOURCE_DEF_COLNAME_SQL = """
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          
        ORDER BY ordinal_position"""


GREENPLUM_TARGET_DEF_COLNAME_SQL = """
        SELECT LOWER(column_name) AS column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          
        ORDER BY ordinal_position"""


GREENPLUM_SOURCE_DEF_IGNORED_COLNAME_SQL = """
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          AND column_name NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY ordinal_position"""


GREENPLUM_TARGET_DEF_IGNORED_COLNAME_SQL = """
        SELECT LOWER(column_name) AS column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('myschema')
          AND LOWER(table_name) = LOWER('mytable')
          AND UPPER(data_type) NOT IN ('IMAGE')
          AND column_name NOT IN ('ignoredcol0', 'ignoredcol1')
        ORDER BY ordinal_position"""

ORACLE_STRING_REPLACE_TEMPLATE = """REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({colname}, CHR(30), ''), CHR(29), ''), CHR(13), ''), CHR(10), ''), CHR(2), ''), CHR(0), '')"""
ORACLE_COL1_REPLACE_CONTROL_CHARS_SQL = ORACLE_STRING_REPLACE_TEMPLATE.format(colname="col1")
ORACLE_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL = ORACLE_STRING_REPLACE_TEMPLATE.format(colname='"col1"')
ORACLE_TEXT_REPLACE_CONTROL_CHARS_SQL = ORACLE_STRING_REPLACE_TEMPLATE.format(colname="textcol")
ORACLE_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL = ORACLE_STRING_REPLACE_TEMPLATE.format(colname='"textcol"')

MSSQL_STRING_REPLACE_TEMPLATE = """NULLIF(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({colname} COLLATE Latin1_General_BIN, CHAR(30), ''), CHAR(29), ''), CHAR(13), ''), CHAR(10), ''), CHAR(2), ''), CHAR(0), ''), '')"""
MSSQL_COL1_REPLACE_CONTROL_CHARS_SQL = MSSQL_STRING_REPLACE_TEMPLATE.format(colname="col1")
MSSQL_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL = MSSQL_STRING_REPLACE_TEMPLATE.format(colname='[col1]')
MSSQL_TEXT_REPLACE_CONTROL_CHARS_SQL = MSSQL_STRING_REPLACE_TEMPLATE.format(colname="CAST(textcol AS VARCHAR)")
MSSQL_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL = MSSQL_STRING_REPLACE_TEMPLATE.format(colname='CAST([textcol] AS VARCHAR)')

POSTGRES_STRING_REPLACE_TEMPLATE = """REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({colname}, CHR(30), ''), CHR(29), ''), CHR(13), ''), CHR(10), ''), CHR(2), '')"""
POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL = POSTGRES_STRING_REPLACE_TEMPLATE.format(colname="col1")
POSTGRES_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL = POSTGRES_STRING_REPLACE_TEMPLATE.format(colname='"col1"')
POSTGRES_TEXT_REPLACE_CONTROL_CHARS_SQL = POSTGRES_STRING_REPLACE_TEMPLATE.format(colname="textcol")
POSTGRES_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL = POSTGRES_STRING_REPLACE_TEMPLATE.format(colname='"textcol"')

GREENPLUM_COL1_REPLACE_CONTROL_CHARS_SQL = POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL
GREENPLUM_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL = POSTGRES_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL
GREENPLUM_TEXT_REPLACE_CONTROL_CHARS_SQL = POSTGRES_TEXT_REPLACE_CONTROL_CHARS_SQL
GREENPLUM_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL = POSTGRES_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL


# Column SQL
MSSQL_SQL_TEMPLATE = "COALESCE(CAST({} as varchar), '')"
MSSQL_WRAPPED_SQL_TEMPLATE = "COALESCE(CAST([{}] as varchar), '')"

MSSQL_INT_COL_SQL_WITH_SLASHES = MSSQL_SQL_TEMPLATE.format("/col2\\")
MSSQL_WRAPPED_INT_COL_SQL_WITH_SLASHES = MSSQL_WRAPPED_SQL_TEMPLATE.format("/col2\\")

MSSQL_BIT_COL_SQL = "COALESCE(CAST(CAST(bitcol AS TINYINT) as varchar), '')"
MSSQL_WRAPPED_BIT_COL_SQL = "COALESCE(CAST(CAST([bitcol] AS TINYINT) as varchar), '')"

MSSQL_FROM_COL_SQL = MSSQL_SQL_TEMPLATE.format("from")
MSSQL_WRAPPED_FROM_COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("from")

MSSQL_FROM__COL_SQL = MSSQL_SQL_TEMPLATE.format("from_")
MSSQL_WRAPPED_FROM__COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("from_")

MSSQL_FROMM_COL_SQL = MSSQL_SQL_TEMPLATE.format("fromm")
MSSQL_WRAPPED_FROMM_COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("fromm")

MSSQL_TO_COL_SQL = MSSQL_SQL_TEMPLATE.format("to")
MSSQL_WRAPPED_TO_COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("to")

MSSQL_TO__COL_SQL = MSSQL_SQL_TEMPLATE.format("to_")
MSSQL_WRAPPED_TO__COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("to_")

MSSQL_IGNORED0_COL_SQL = MSSQL_SQL_TEMPLATE.format("ignoredcol0")
MSSQL_WRAPPED_IGNORED0_COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("ignoredcol0")
MSSQL_IGNORED1_COL_SQL = MSSQL_SQL_TEMPLATE.format("ignoredcol1")
MSSQL_WRAPPED_IGNORED1_COL_SQL = MSSQL_WRAPPED_SQL_TEMPLATE.format("ignoredcol1")

tests=[

    # Oracle

    TestCase(
        description="Oracle (definition) -> Postgres",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[ORACLE_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: ORACLE_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle wrapped (definition) -> Postgres",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[ORACLE_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: ORACLE_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to_"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol0"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol1"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle (definition) -> Postgres ignored columns",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[ORACLE_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: ORACLE_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle wrapped (definition) -> Postgres ignored columns",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[ORACLE_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: ORACLE_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to_"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle -> Postgres (definition)",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle -> Postgres (definition) ignored columns",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle wrapped -> Postgres (definition)",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol0"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol1"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Oracle wrapped -> Postgres (definition) ignored columns",
        input_source_dbtype=const.ORACLE,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: ORACLE_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    # Mssql
    TestCase(
        description="Mssql (definition) -> Postgres",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[MSSQL_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: MSSQL_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TO__COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_IGNORED0_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_IGNORED1_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Mssql wrapped (definition) -> Postgres",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[MSSQL_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: MSSQL_WRAPPED_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_WRAPPED_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TO__COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_IGNORED0_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_IGNORED1_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),
    
    TestCase(
        description="Mssql (definition) -> Postgres ignored columns",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[MSSQL_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: MSSQL_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TO__COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Mssql wrapped (definition) -> Postgres ignored columns",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[MSSQL_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: MSSQL_WRAPPED_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_WRAPPED_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TO__COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Mssql -> Postgres (definition)",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TO_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_IGNORED0_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_IGNORED1_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Mssql -> Postgres (definition) ignored columns",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL
        ],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_TO_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Mssql wrapped -> Postgres (definition)",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_WRAPPED_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_WRAPPED_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TO_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_IGNORED0_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_IGNORED1_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Mssql wrapped -> Postgres (definition) ignored columns",
        input_source_dbtype=const.MSSQL,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL
        ],
        expected_source_cols=[
            {const.FIELD_NAME: MSSQL_WRAPPED_INT_COL_SQL_WITH_SLASHES, const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: MSSQL_WRAPPED_BIT_COL_SQL, const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_FROMM_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: MSSQL_WRAPPED_TO_COL_SQL, const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),


    # Postgres

    TestCase(
        description="Postgres (definition) -> Postgres",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[POSTGRES_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres wrapped (definition) -> Postgres",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[POSTGRES_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: POSTGRES_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to_"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol0"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol1"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres (definition) -> Postgres ignored columns",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: POSTGRES_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres wrapped (definition) -> Postgres ignored columns",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: POSTGRES_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to_"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres -> Postgres (definition)",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres -> Postgres (definition) ignored columns",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres wrapped -> Postgres (definition)",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol0"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol1"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Postgres wrapped -> Postgres (definition) ignored columns",
        input_source_dbtype=const.POSTGRES,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: POSTGRES_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),


    # Greenplum

    TestCase(
        description="Greenplum (definition) -> Postgres",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[GREENPLUM_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: GREENPLUM_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum wrapped (definition) -> Postgres",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=None,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[GREENPLUM_SOURCE_DEF_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: GREENPLUM_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to_"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol0"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol1"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum (definition) -> Postgres ignored columns",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[GREENPLUM_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: GREENPLUM_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum wrapped (definition) -> Postgres ignored columns",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.SOURCE,
        expected_sql_calls_on_definition_db=[GREENPLUM_SOURCE_DEF_IGNORED_COLNAME_SQL],
        expected_source_cols=[
            {const.FIELD_NAME: GREENPLUM_WRAPPED_COL1_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to_"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: 'col1', const.DATA_TYPE: 'VARCHAR', const.PARAMS: ['10']},
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum -> Postgres (definition)",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum -> Postgres (definition) ignored columns",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=False,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum wrapped -> Postgres (definition)",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=None,
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_COLNAME_SQL,
            POSTGRES_TARGET_DEF_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol0"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"ignoredcol1"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol0', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'ignoredcol1', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    TestCase(
        description="Greenplum wrapped -> Postgres (definition) ignored columns",
        input_source_dbtype=const.GREENPLUM,
        input_target_dbtype=const.POSTGRES,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            POSTGRES_SOURCE_DEF_IGNORED_COLNAME_SQL,
            POSTGRES_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '"/col2\\"', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: '"bitcol"', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: GREENPLUM_WRAPPED_TEXT_REPLACE_CONTROL_CHARS_SQL, const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: '"from"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"fromm"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: '"to"', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),

    # File
    TestCase(
        description="File -> Greenplum (definition) ignored columns",
        input_source_dbtype=const.FILE,
        input_target_dbtype=const.GREENPLUM,
        input_nonstandardcolumnnames=True,
        input_definition_origin=const.TARGET,
        input_ignored_columns=["ignoredcol0", "ignoredcol1"],
        expected_sql_calls_on_definition_db=[
            GREENPLUM_SOURCE_DEF_IGNORED_COLNAME_SQL,
            GREENPLUM_TARGET_DEF_IGNORED_COLNAME_SQL,
        ],
        expected_source_cols=[
            {const.FIELD_NAME: '/col2\\', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
        expected_target_cols=[
            {const.FIELD_NAME: '_col2_', const.DATA_TYPE: 'NUMBER', const.PARAMS: ['3', '0']},
            {const.FIELD_NAME: 'bitcol', const.DATA_TYPE: 'BIT', const.PARAMS: []},
            {const.FIELD_NAME: 'textcol', const.DATA_TYPE: 'TEXT', const.PARAMS: []},
            {const.FIELD_NAME: 'from_', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'fromm', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
            {const.FIELD_NAME: 'to', const.DATA_TYPE: 'INTEGER', const.PARAMS: []},
        ],
    ),
]
