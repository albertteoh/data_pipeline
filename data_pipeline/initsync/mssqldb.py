###############################################################################
# Module:    mssqldb
# Purpose:   Contains mssql specific initsync functions
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const

from .exceptions import NotSupportedException
from .sqldb import SqlDb


class MssqlDb(SqlDb):
    def __init__(self, argv, db, logger):
        super(MssqlDb, self).__init__(argv, db, logger)

    def _is_string_type(self, datatype):
        datatype = datatype.upper()
        return (datatype in ['TEXT'] or
                'CHAR' in datatype)

    def _get_ascii_function(self):
        return 'CHAR'

    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        modified_sql = colname

        if datatype.upper() == 'BIT':
            modified_sql = "CAST({colname} AS TINYINT)".format(colname=colname)

        # Explicitly set any NULL values to the configured nullstring
        # to avoid the invalid integer "" error by COPY command whilst
        # allowing for empty strings to be supplied. Required for BCP loads.
        return ("COALESCE(CAST({modified_sql} as varchar), '{nullstring}')"
                .format(modified_sql=modified_sql,
                        nullstring=self._argv.nullstring))

    def _is_valid_char_num(self, char_num):
        return True

    def _build_colname_sql(self, table, lowercase):
        column_name_name = "[column_name]"
        ignore_cols_sql = self._build_ignore_columns_sql(column_name_name)
        colname_select = self._build_colname_select(lowercase, column_name_name)

        sqlstr = ("""
        SELECT {colname_select}, [data_type], [character_maximum_length], [numeric_precision], [numeric_scale]
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER([table_schema]) = LOWER('{source_schema}')
          AND LOWER([table_name]) = LOWER('{table_name}')
          AND UPPER([data_type]) NOT IN ('IMAGE', 'VARBINARY')
          {and_not_in_ignored_columns}
        ORDER BY ordinal_position""".format(
            colname_select=colname_select,
            source_schema=table.schema,
            table_name=table.name,
            and_not_in_ignored_columns=ignore_cols_sql,
        ))
        return sqlstr

    def _pre_extract(self):
        pass

    def _post_extract(self, record):
        return record

    def _wrap_colname(self, colname):
        return '[{}]'.format(colname)

    def _pre_wrap_with_ascii_replace(self, colname, datatype):
        """SQL Server don't support REPLACE on the TEXT data type (which is
        being deprecated), so we'll need to CAST it to VARCHAR first.

        Also add collate clause to the column to workaround a SQL Server bug:
        https://stackoverflow.com/questions/2298412/replace-null-character-in-a-string-in-sql
        """
        colsql = colname
        if datatype.upper() == "TEXT":
            colsql = "CAST({} AS VARCHAR)".format(colname)

        return ("{colsql} COLLATE Latin1_General_BIN"
                .format(colsql=colsql))

    def _post_wrap_with_ascii_replace(self, colsql, datatype):
        """Make sure we don't return any emptystrings back from query as these
        result in \x00 (NUL) characters being written to file by bcp, which
        the PG/GP COPY command doesn't like. NULL values, however, are
        converted to an empty string by bcp.
        """
        return "NULLIF({colsql}, '')".format(colsql=colsql)

    def build_extract_data_sql(self, column_list, table, extractlsn,
                               samplerows, lock, query_condition):
        # We only want the list of column names
        colnames = self._get_colnames_from_column_list(column_list)

        withnolock = const.EMPTY_STRING if lock else "WITH (NOLOCK)"
        top = const.EMPTY_STRING

        if samplerows > 0:
            top = "TOP {rows}".format(rows=samplerows)

        extractlsn_sql = const.EMPTY_STRING
        if extractlsn:
            extractlsn_sql = """
            , (SELECT CONVERT(VARCHAR(50), SYS.FN_CDC_GET_MAX_LSN(), 2)) AS LSN"""

        metacols_sql = self._build_metacols_sql()

        sql = """
        SELECT {top}
              {columns}{metacols_sql}{extractlsn_sql}
        FROM {table}
        {withnolock}
        WHERE 1=1""".format(top=top,
                            columns="\n            , ".join(colnames),
                            metacols_sql=metacols_sql,
                            extractlsn_sql=extractlsn_sql,
                            table=table.fullname,
                            withnolock=withnolock)

        sql = self._append_query_condition(sql, query_condition, table)
        return sql

    def table_exists(self, table):
        table_exists_sql = self._build_table_exists_sql(table)
        result = self._db.execute_query(
            table_exists_sql, const.DEFAULT_ARRAYSIZE)
        row = result.fetchone()

        return int(row[0]) > 0 if row is not None else False

    def _build_table_exists_sql(self, table):
        return """
        IF EXISTS (
          SELECT * FROM INFORMATION_SCHEMA.TABLES
          WHERE LOWER([table_schema]) = LOWER('{schema}')
          AND   LOWER([table_name]) = LOWER('{table_name}')
        )
          BEGIN
            SELECT 1
          END
        ELSE
          BEGIN
            SELECT 0
          END""".format(schema=table.schema, table_name=table.name)

    def bulk_write(self, **kwargs):
        raise NotSupportedException(
            "bulk_write currently not supported for Mssql")

    def _build_analyze_sql(self, table):
        raise NotSupportedException(
            "build_analyze_sql currently not supported for Mssql")

    def _build_truncate_sql(self, table):
        raise NotSupportedException(
            "build_truncate_sql currently not supported for Mssql")

    def _build_vacuum_sql(self, table):
        raise NotSupportedException(
            "_build_vacuum_sql currently not supported for Mssql")
