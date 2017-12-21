##################################################################################
# Module:   db_data_dictionary
# Purpose:  Database specific data dictionary helper
#
# Notes:
#
##################################################################################


import data_pipeline.constants.const as const


def getSQLForTableList(conn_db_raw, schema_regexp, table_regexp):

    if conn_db_raw.dbtype == const.ORACLE:
        sql = """select owner, table_name
        from all_tables atbl
        where atbl.owner = '{}'
        and regexp_like(table_name, '{}')
        order by 1,2 """.format(schema_regexp, table_regexp)

    elif conn_db_raw.dbtype == const.POSTGRES:
        sql = """select table_schema, table_name
        from information_schema.tables atbl
        where atbl.table_schema = '{}'
        and table_name ~ '{}' 
        order by 1,2 """.format(schema_regexp, table_regexp)

    else:
        raise ValueError('Database type "{}" not implemented'.format(conn_db_raw.dbtype))

    return sql

