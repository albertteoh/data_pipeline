###############################################################################
# Module:    postgresdb
# Purpose:   Contains postgres specific initsync functions
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const

from .exceptions import NotSupportedException
from .sqldb import SqlDb


class PostgresDb(SqlDb):
    def __init__(self, argv, db, logger):
        super(PostgresDb, self).__init__(argv, db, logger)

    def _is_string_type(self, datatype):
        datatype = datatype.upper()
        return (datatype in ['TEXT'] or
                'CHAR' in datatype)

    def _get_ascii_function(self):
        return 'CHR'

    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        return colname

    def _is_valid_char_num(self, char_num):
        # Postgres errors on null char with: "null character not permitted"
        return char_num not in [const.ASCII_NULL]

    def _build_colname_sql(self, table, lowercase):
        column_name_name = "column_name"
        ignore_cols_sql = self._build_ignore_columns_sql(column_name_name)
        colname_select = self._build_colname_select(lowercase, column_name_name)

        sqlstr = ("""
        SELECT {colname_select}, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM  INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
          AND LOWER(table_schema) = LOWER('{source_schema}')
          AND LOWER(table_name) = LOWER('{table_name}')
          AND UPPER(data_type) NOT IN ('IMAGE')
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
        return '"{}"'.format(colname)

    def _pre_wrap_with_ascii_replace(self, colname, datatype):
        return colname

    def _post_wrap_with_ascii_replace(self, colname, datatype):
        return colname

    def build_extract_data_sql(self, column_list, table, extractlsn,
                               samplerows, lock, query_condition):
        # We only want the list of column names
        colnames = self._get_colnames_from_column_list(column_list)

        extractlsn_sql = const.EMPTY_STRING
        if extractlsn:
            extractlsn_sql = """
            , 'UNSUPPORTED BY DB'"""

        metacols_sql = self._build_metacols_sql()

        sql = """
        SELECT
              {columns}{metacols_sql}{extractlsn_sql}
        FROM {table}
        WHERE 1=1""".format(columns="\n            , ".join(colnames),
                            metacols_sql=metacols_sql,
                            extractlsn_sql=extractlsn_sql,
                            table=table.fullname)

        sql = self._append_query_condition(sql, query_condition, table)
        sql = self._append_samplerows(sql, samplerows)
        return sql

    def _append_samplerows(self, sql, samplerows):
        if samplerows > 0:
            sql += """
        LIMIT {rows}""".format(rows=samplerows)

        return sql

    def table_exists(self, table):
        table_exists_sql = self._build_table_exists_sql(table)
        result = self._db.execute_query(
            table_exists_sql,
            const.DEFAULT_ARRAYSIZE)
        row = result.fetchone()
        if row is None:
            return False

        (exists, ) = row
        return exists

    def _build_table_exists_sql(self, table):
        return """
        SELECT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{schema}'
              AND table_name   = '{table_name}'
        )""".format(schema=table.schema.lower(), table_name=table.name.lower())

    def bulk_write(self, host, port, user, password, database,
                   query_condition, **kwargs):
        column_list = kwargs['column_list']

        if column_list:
            # We only want the list of column names
            colnames = self._get_colnames_from_column_list(column_list)
            kwargs['column_list'] = list(colnames)

        return self._db.copy_expert(**kwargs)

    def _build_analyze_sql(self, table):
        return ("ANALYZE {full_table_name}"
                .format(full_table_name=table.fullname))

    def _build_truncate_sql(self, table):
        return ("TRUNCATE {full_table_name}"
                .format(full_table_name=table.fullname))

    def _build_vacuum_sql(self, table):
        return ("VACUUM FULL {full_table_name}"
                .format(full_table_name=table.fullname))
