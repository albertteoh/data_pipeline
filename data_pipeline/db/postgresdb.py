###############################################################################
# Module:    postgresdb
# Purpose:   Wraps the native Postgres client, exposing required operations on
#            Postgres databases
#
# Notes:
#
###############################################################################

import psycopg2
import logging
import re
import data_pipeline.constants.const as const

from .db import Db
from .db_query_results import DbQueryResults


class PostgresDb(Db):
    def __init__(self):
        super(PostgresDb, self).__init__()

    @property
    def dbtype(self):
        return const.POSTGRES

    def _connect(self, conn_details):
        self._connection_details = conn_details

        sslmode = const.EMPTY_STRING
        if conn_details.sslmode:
            sslmode = "sslmode='{}'".format(conn_details.sslmode)

        sslcert = const.EMPTY_STRING
        if conn_details.sslcert:
            sslcert = "sslcert='{}'".format(conn_details.sslcert)

        sslrootcert = const.EMPTY_STRING
        if conn_details.sslrootcert:
            sslrootcert = "sslrootcert='{}'".format(conn_details.sslrootcert)

        sslkey = const.EMPTY_STRING
        if conn_details.sslkey:
            sslkey = "sslkey='{}'".format(conn_details.sslkey)

        sslcrl = const.EMPTY_STRING
        if conn_details.sslcrl:
            sslcrl = "sslcrl='{}'".format(conn_details.sslcrl)

        conn_string = (
            "host='{host}' dbname='{dbname}' user='{user}' "
            "password='{password}' port={port} "
            "{sslmode} {sslcert} {sslrootcert} {sslkey} {sslcrl}"
            .format(host=conn_details.host,
                    dbname=conn_details.dbsid,
                    user=conn_details.userid,
                    password=conn_details.password,
                    port=conn_details.port,
                    sslmode=sslmode,
                    sslcert=sslcert,
                    sslrootcert=sslrootcert,
                    sslkey=sslkey,
                    sslcrl=sslcrl))

        self._logger.info("Connecting to database -> {}"
                          .format(re.sub(r'password=.*? ', r'', conn_string)))

        self._connection = psycopg2.connect(conn_string)

        # Explicitly set autocommit
        self._connection.autocommit = False

        self._cursor = self._connection.cursor()
        self._logger.info("Connection successful")

    def execute_stored_proc(self, stored_proc):
        pass

    def execute_query(self, query, arraysize, values=(),
                      post_process_func=None):
        self._logger.debug("Executing query '{q}'\nBind values = {v}"
                           .format(q=query, v=values))
        self._cursor.arraysize = arraysize
        self._cursor.execute(query, values)
        query_results = DbQueryResults(self._cursor)
        self._logger.debug("Query execution complete")
        return query_results

    def copy_expert(self, input_file, table_name, sep, null_string,
                    column_list, quote_char, escape_char, size, header):

        column_list_str = const.EMPTY_STRING
        if column_list:
            column_list_str = "( {} )".format(const.COMMA.join(column_list))

        copy_command = """
            COPY {table_name} {column_list}
            FROM STDIN
            DELIMITER '{sep}' {text_or_csv}
            NULL '{null}'
            QUOTE '{quote}'
            ESCAPE '{escape}'
            {header}
        """.format(table_name=str(table_name),
                   column_list=column_list_str,
                   sep=sep,
                   text_or_csv="CSV",
                   null=null_string,
                   quote=quote_char,
                   escape=escape_char,
                   header="HEADER" if header else const.EMPTY_STRING,
           )

        self._logger.debug("Executing copy_expert({cmd}, size={s})"
                           .format(cmd=copy_command, s=size))

        self._cursor.copy_expert(copy_command, input_file, size=size)

        return self._cursor.rowcount

    def copy_from(self, input_file, table_name, sep,
                  null_string, column_list, size):

        columns = const.COMMA.join(column_list)

        self._logger.debug("Executing copy_from({table}, sep={sep}, "
                           "null={null}, columns={columns}, size={size}"
                           .format(table=table_name, sep=sep, null=null_string,
                                   columns=columns))
        self._cursor.copy_from(
            input_file, table_name, sep=sep,
            null=null_string, columns=columns,
            size=size)

        return self._cursor.rowcount

    def commit(self):
        self._connection.commit()

    def is_client_present(self):
        pass

    def __del__(self):
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
