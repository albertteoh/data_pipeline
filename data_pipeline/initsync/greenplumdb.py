###############################################################################
# Module:    greenplumdb
# Purpose:   Contains greenplum specific initsync functions. Given greenplum
#            and postgres essentially share the same API, this module should
#            just inherit all functions from postgresdb
#
# Notes:
#
###############################################################################


import os
import re
import shlex
import subprocess
import yaml
import data_pipeline.constants.const as const

from postgresdb import PostgresDb


class GreenplumDb(PostgresDb):
    def __init__(self, argv, db, logger):
        super(GreenplumDb, self).__init__(argv, db, logger)

    def bulk_write(self,**kwargs):
        if self._argv.directload == const.GPLOAD:
            return self._gpload(**kwargs)
        return super(GreenplumDb, self).bulk_write(**kwargs)

    def _gpload(self, database, user, password, host, port, query_condition,
                input_file, table_name, sep, null_string, column_list,
                quote_char, escape_char, **kwargs):

        self._logger.info("Starting gpload...")
        if self._argv.localhost is None:
            raise Exception("Localhost is required for gpload. Please "
                            "set this in the configuration.")

        # Build up the gpload configuration

        # If we assume column definitions match between
        # source and target, then ensure we don't print the columns
        if self._argv.assumematchingcolumns:
            columns_yaml = const.EMPTY_STRING
        else:
            columns_yaml = """- COLUMNS:
"""
            for column_details in column_list:
                columns_yaml += ('      - "{colname}": {datatype}\n'.format(
                                 colname=column_details[const.FIELD_NAME],
                                 datatype=column_details[const.DATA_TYPE]))

        delete_sql = const.EMPTY_STRING
        if self._argv.delete:
            delete_sql = self._build_delete_sql(table_name, query_condition)

        gpload_config = """
VERSION: 1.0.0.1
DATABASE: {database}
USER: {user}
HOST: {host}
PORT: {port}
GPLOAD:
  INPUT: 
    - SOURCE:
        LOCAL_HOSTNAME:
          - {localhost}
        PORT_RANGE: [{portstart}, {portend}]
        FILE:
          - {input_file}
    - FORMAT: TEXT
    - DELIMITER: {delimiter}
    - ESCAPE: 'OFF'
    - NULL_AS: '{nullstring}'
    - QUOTE: {quotechar}
    - HEADER: {header}
    - ENCODING: {clientencoding}
    - ERROR_LIMIT: 100
    - LOG_ERRORS: false
    {columns}
  OUTPUT:
    - TABLE: {full_tablename}
    - MODE: insert
  PRELOAD:
    - TRUNCATE: {truncate}
    - REUSE_TABLES: false
  SQL:
    - BEFORE: {delete}
    - AFTER:
""".format(
        database=database,
        user=user,
        host=host,
        port=port,
        localhost=self._argv.localhost,
        portstart=self._argv.portrange[0],
        portend=self._argv.portrange[1],
        input_file=input_file.name,
        delimiter=repr(sep), # for special chars
        nullstring=null_string,
        quotechar=repr(quote_char), # for special chars
        header=str(self._argv.header).lower(),
        clientencoding=self._argv.clientencoding,
        columns=columns_yaml,
        full_tablename=table_name.fullname,
        truncate=self._argv.truncate,
        delete=delete_sql,
        )

        gpload_config_filename = os.path.join(
            self._argv.workdirectory,
            "{table}_gpload_config.yaml".format(
                table=table_name.name
            ),
        )

        with open(gpload_config_filename, 'w') as gpload_config_file:
            gpload_config_file.write(gpload_config)

        self._logger.info(
            "Wrote {config}"
            .format(config=gpload_config_file)
        )

        stdout_file = os.path.join(self._argv.workdirectory,
                                   "{}_gpload.out".format(table_name.name))

        cmd = (
            "gpload -f {config} --gpfdist_timeout 300 -l {logfile} -V -v"
            .format(
                config=gpload_config_filename,
                logfile=stdout_file,
            )
        )

        self._logger.info("Executing external program: {cmd}"
                          .format(cmd=cmd))
        args = shlex.split(cmd)
        p = subprocess.Popen(args, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        errcode = p.returncode

        if errcode != 0:
            raise Exception("""
Failed execution of: {cmd}
ERRCODE: {errcode}
STDOUT: {out}
STDERR: {err}"""
                .format(cmd=cmd, errcode=errcode, out=out, err=err)
            )


        gpload_summary_pattern = """
^.+INFO\|rows Inserted          = (\d+)
^.+INFO\|rows Updated           = (\d+)
^.+INFO\|data formatting errors = (\d+)"""

        m = re.search(gpload_summary_pattern, out, re.MULTILINE)
        if m:
            formatting_errors_count = int(m.group(3))
            if formatting_errors_count > 0:
                raise Exception("""
Failed execution of: {cmd}
There were {errs} data formatting errors. Please refer to gpload temporary
tables to view error logs."""
                .format(cmd=cmd, errs=formatting_errors_count)
                )

            rowcount = m.group(1)
            return int(rowcount)

        return 0
