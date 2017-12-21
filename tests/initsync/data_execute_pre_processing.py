import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple("TestCase", "description loaddefinition datatypemap source_dbtype target_dbtype column_list pk_list query_condition delete truncate droptable droptablecascade createtable expected_sql")


DELETE_SQL = """
        DELETE FROM myschema.mytable
        WHERE 1=1"""


DELETE_QUERY_CONDITION_SQL = """
        DELETE FROM myschema.mytable
        WHERE 1=1
          AND col1 like '%foo%'"""


DELETE_QUERY_TOKEN_CONDITION_SQL = """
        DELETE FROM myschema.mytable
        WHERE 1=1
          AND col1 > (select (MAX(col1) - 10) from myschema.mytable)"""


TRUNCATE_SQL = "TRUNCATE myschema.mytable"


DROP_TABLE_SQL = "DROP TABLE IF EXISTS myschema.mytable"


DROP_TABLE_CASCADE_SQL = "DROP TABLE IF EXISTS myschema.mytable CASCADE"


ORACLE_POSTGRES_CREATE_TABLE_SQL = "CREATE TABLE ctl.mytable (col1 VARCHAR(10), _col2_ SMALLINT, PRIMARY KEY (col1, _col2_))"
ORACLE_GREENPLUM_CREATE_TABLE_SQL = "CREATE TABLE ctl.mytable (col1 VARCHAR(10), _col2_ SMALLINT, PRIMARY KEY (col1, _col2_)) DISTRIBUTED BY (col1, _col2_)"
ORACLE_GREENPLUM_CREATE_TABLE_NO_PK_SQL = "CREATE TABLE ctl.mytable (col1 VARCHAR(10), _col2_ SMALLINT)"

MSSQL_POSTGRES_CREATE_TABLE_SQL = "CREATE TABLE ctl.mytable (col1 VARCHAR(10), _col2_ NUMERIC(3, 0), PRIMARY KEY (col1, _col2_))"
MSSQL_GREENPLUM_CREATE_TABLE_SQL = "CREATE TABLE ctl.mytable (col1 VARCHAR(10), _col2_ NUMERIC(3, 0), PRIMARY KEY (col1, _col2_)) DISTRIBUTED BY (col1, _col2_)"
MSSQL_GREENPLUM_CREATE_TABLE_NO_PK_SQL = "CREATE TABLE ctl.mytable (col1 VARCHAR(10), _col2_ NUMERIC(3, 0))"

POSTGRES_POSTGRES_CREATE_TABLE_SQL = MSSQL_POSTGRES_CREATE_TABLE_SQL
POSTGRES_GREENPLUM_CREATE_TABLE_SQL = MSSQL_GREENPLUM_CREATE_TABLE_SQL
POSTGRES_GREENPLUM_CREATE_TABLE_NO_PK_SQL = MSSQL_GREENPLUM_CREATE_TABLE_NO_PK_SQL

GREENPLUM_POSTGRES_CREATE_TABLE_SQL = MSSQL_POSTGRES_CREATE_TABLE_SQL
GREENPLUM_GREENPLUM_CREATE_TABLE_SQL = MSSQL_GREENPLUM_CREATE_TABLE_SQL
GREENPLUM_GREENPLUM_CREATE_TABLE_NO_PK_SQL = MSSQL_GREENPLUM_CREATE_TABLE_NO_PK_SQL

MOCK_ORACLE_COLUMN_LIST = [
    {
        const.FIELD_NAME: "col1",
        const.DATA_TYPE: "VARCHAR",
        const.PARAMS: ["10"],
    },
    {
        const.FIELD_NAME: "_col2_",
        const.DATA_TYPE: "NUMBER",
        const.PARAMS: ["3", "0"],
    },
]


MOCK_GREENPLUM_COLUMN_LIST = [
    {
        const.FIELD_NAME: "col1",
        const.DATA_TYPE: "VARCHAR",
        const.PARAMS: ["10"],
    },
    {
        const.FIELD_NAME: "_col2_",
        const.DATA_TYPE: "NUMERIC",
        const.PARAMS: ["3", "0"],
    },
]


MOCK_MSSQL_COLUMN_LIST = [
    {
        const.FIELD_NAME: "col1",
        const.DATA_TYPE: "NVARCHAR",
        const.PARAMS: ["10"],
    },
    {
        const.FIELD_NAME: "_col2_",
        const.DATA_TYPE: "NUMERIC",
        const.PARAMS: ["3", "0"],
    },
]


POSTGRES_DATATYPEMAP = "conf/postgres_datatype_mappings.yaml"

TWO_PKS_LIST = ["col1", "_col2_"]
EMPTY_PKS_LIST = []


tests = [
    TestCase(
        description="Delete",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=True,
        truncate=False,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DELETE_SQL],
    ),

    TestCase(
        description="Delete with query condition",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition="col1 like '%foo%'",
        delete=True,
        truncate=False,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DELETE_QUERY_CONDITION_SQL],
    ),

    TestCase(
        description="Delete with query condition",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition="col1 > (select (MAX(col1) - 10) from {SCHEMA}.{TABLE})",
        delete=True,
        truncate=False,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DELETE_QUERY_TOKEN_CONDITION_SQL],
    ),

    TestCase(
        description="Delete and truncate should delete",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=True,
        truncate=True,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DELETE_SQL],
    ),

    TestCase(
        description="Delete and truncate with query condition should delete",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition="col1 like '%foo%'",
        delete=True,
        truncate=True,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DELETE_QUERY_CONDITION_SQL],
    ),

    TestCase(
        description="Delete and truncate with tokenised query condition should delete",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition="col1 > (select (MAX(col1) - 10) from {SCHEMA}.{TABLE})",
        delete=True,
        truncate=True,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DELETE_QUERY_TOKEN_CONDITION_SQL],
    ),

    TestCase(
        description="Truncate",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=True,
        droptable=False,
        droptablecascade=False,
        createtable=False,
        expected_sql=[TRUNCATE_SQL],
    ),

    TestCase(
        description="Drop",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=False,
        createtable=False,
        expected_sql=[DROP_TABLE_SQL],
    ),

    TestCase(
        description="Drop cascade",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=False,
        droptablecascade=True,
        createtable=False,
        expected_sql=[DROP_TABLE_CASCADE_SQL],
    ),

    TestCase(
        description="Drop and dropcascade should favour drop cascade",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=True,
        createtable=False,
        expected_sql=[DROP_TABLE_CASCADE_SQL],
    ),

    TestCase(
        description="Dropcascade and create table oracle->postgres",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.POSTGRES,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=True,
        createtable=True,
        expected_sql=[DROP_TABLE_CASCADE_SQL, ORACLE_POSTGRES_CREATE_TABLE_SQL],
    ),

    TestCase(
        description="Dropcascade and create table oracle->greenplum",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.GREENPLUM,
        column_list=MOCK_ORACLE_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=True,
        createtable=True,
        expected_sql=[DROP_TABLE_CASCADE_SQL, ORACLE_GREENPLUM_CREATE_TABLE_SQL],
    ),

    TestCase(
        description="Dropcascade and create table greenplum->greenplum",
        loaddefinition=const.TARGET,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.ORACLE,
        target_dbtype=const.GREENPLUM,
        column_list=MOCK_GREENPLUM_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=True,
        createtable=True,
        expected_sql=[DROP_TABLE_CASCADE_SQL, GREENPLUM_GREENPLUM_CREATE_TABLE_SQL],
    ),

    TestCase(
        description="Dropcascade and create table mssql->greenplum",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.MSSQL,
        target_dbtype=const.GREENPLUM,
        column_list=MOCK_MSSQL_COLUMN_LIST,
        pk_list=TWO_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=True,
        createtable=True,
        expected_sql=[DROP_TABLE_CASCADE_SQL, MSSQL_GREENPLUM_CREATE_TABLE_SQL],
    ),

    TestCase(
        description="Dropcascade and create table mssql->greenplum no pk",
        loaddefinition=const.SOURCE,
        datatypemap=POSTGRES_DATATYPEMAP,
        source_dbtype=const.MSSQL,
        target_dbtype=const.GREENPLUM,
        column_list=MOCK_MSSQL_COLUMN_LIST,
        pk_list=EMPTY_PKS_LIST,
        query_condition=None,
        delete=False,
        truncate=False,
        droptable=True,
        droptablecascade=True,
        createtable=True,
        expected_sql=[DROP_TABLE_CASCADE_SQL, MSSQL_GREENPLUM_CREATE_TABLE_NO_PK_SQL],
    ),


]
