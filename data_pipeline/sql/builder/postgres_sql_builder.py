###############################################################################
# Module:    postgres_sql_builder
# Purpose:   Builds generic sql statements for postgres db
#
# Notes:     This module focuses on logic for applying CDCs, often involving
#            execution of individual SQL statements
#
###############################################################################

import logging
import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod
from .sql_builder import SqlBuilder


class PostgresSqlBuilder(SqlBuilder):
    def __init__(self, argv):
        super(PostgresSqlBuilder, self).__init__(argv)
        self._logger = logging.getLogger(__name__)

    def build_keycolumnlist_sql(self, schemas, tables):
        schemas_sql = const.COMMASPACE.join(["'{}'".format(s) for s in schemas])
        tables_sql = const.COMMASPACE.join(["'{}'".format(s) for s in tables])
        sql = """
        SELECT t.table_name, c.column_name
        FROM information_schema.key_column_usage AS c
        LEFT JOIN information_schema.table_constraints AS t
        ON t.constraint_name = c.constraint_name
        WHERE t.constraint_type = 'PRIMARY KEY'
          AND t.table_schema in ({schemas})
          AND t.table_name in ({tables})""".format(
            schemas=schemas_sql,
            tables=tables_sql,
        )
        return sql
