###############################################################################
# Module:    greenplum_sql_builder
# Purpose:   Builds sql statements for Greenplum target dbs
#
# Notes:     Greenplum is essentially a Postgres DB, hence why it inherits from
#            PostgresSqlBuilder. The only differentiator is the need to remove
#            primary keys from the SET clause of an UPDATE statement.
#
###############################################################################


import itertools
import logging
import data_pipeline.constants.const as const

from .postgres_sql_builder import PostgresSqlBuilder

class GreenplumSqlBuilder(PostgresSqlBuilder):
    def __init__(self, argv):
        super(GreenplumSqlBuilder, self).__init__(argv)
        self._logger = logging.getLogger(__name__)

    def build_update_sql(self, update_statement):
    
        # Greenplum doesn't like seeing primary keys in SET clause of UPDATE
        def is_not_a_primary_key(field_name):
            if update_statement.primary_key_list:
                return field_name not in update_statement.primary_key_list
            return True

        self._update_field_filter_func = is_not_a_primary_key

        return super(GreenplumSqlBuilder, self).build_update_sql(update_statement)

    def build_create_sql(self, create_statement):
        sql = super(GreenplumSqlBuilder, self).build_create_sql(create_statement)

        key_entries = [entry.get(const.KEYS) for entry in create_statement.entries
                       if entry.get(const.KEYS)]
        if key_entries:
            keylist = list(itertools.chain.from_iterable(key_entries))
            key_sql = const.COMMASPACE.join(keylist)
            return ("{base_create_sql} DISTRIBUTED BY ({keys})"
                    .format(base_create_sql=sql, keys=key_sql))

        return sql
