###############################################################################
# Module:    oracle_sql_builder
# Purpose:   Builds generic sql statements for oracle db
#
# Notes:     This module focuses on logic for applying CDCs, often involving
#            execution of individual SQL statements
#
###############################################################################

import logging
import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod
from .sql_builder import SqlBuilder


class OracleSqlBuilder(SqlBuilder):
    def __init__(self, argv):
        super(OracleSqlBuilder, self).__init__(argv)
        self._logger = logging.getLogger(__name__)

    def build_keycolumnlist_sql(self, schemas, tables):
        owner_filter = const.EMPTY_STRING
        if schemas:
            schema_list = ["'{schema}'".format(schema=s.upper())
                           for s in schemas]
            csv_schemas = const.COMMASPACE.join(schema_list)
            owner_filter = ("AND UPPER(cons.owner) IN ({schemas})"
                            .format(schemas=csv_schemas))

        table_filter = const.EMPTY_STRING
        if tables:
            table_list = ["'{table_name}'".format(table_name=t.upper())
                          for t in tables]
            csv_tables = const.COMMASPACE.join(table_list)
            table_filter = ("AND UPPER(cons.table_name) IN ({tables})"
                            .format(tables=csv_tables))

        return """
        SELECT cons.table_name tabname, LOWER(column_name) colname
        FROM all_constraints cons, all_cons_columns col
        WHERE cons.owner = col.owner
           AND cons.constraint_name = col.constraint_name
           {owner_filter}
           {table_filter}
           AND cons.constraint_type IN ('P', 'U')
        ORDER BY 1, col.position""".format(
            owner_filter=owner_filter,
            table_filter=table_filter,
        )
