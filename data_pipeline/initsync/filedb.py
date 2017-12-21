###############################################################################
# Module:    filedb
# Purpose:   Contains file specific initsync functions
#
# Notes:
#
###############################################################################


import datetime

import data_pipeline.constants.const as const

from exceptions import NotSupportedException
from initsyncdb import InitSyncDb


class FileDb(InitSyncDb):
    def __init__(self, argv, db, logger, delimiter, quotechar):
        super(FileDb, self).__init__(argv, db, logger)
        self._db.delimiter = delimiter
        self._db.quotechar = quotechar
        self._now_str = datetime.datetime.now().strftime(const.TIMESTAMP_FORMAT)

    def query_columns(self, table, lowercase):
        raise NotSupportedException(
            "query_columns currently not supported for Files")

    def get_decorated_source_column_list(
            self,
            column_list_query_result,
            definition_origin):
        return map(self._build_column_entry, column_list_query_result)

    def get_decorated_target_column_list(
            self,
            column_list_query_result,
            definition_origin):
        return map(self._build_column_entry, column_list_query_result)

    def _build_column_entry(self, row):
        (colname, datatype, params) = row
        column_entry = {
            const.FIELD_NAME: colname,
            const.DATA_TYPE: datatype,
            const.PARAMS: params,
        }
        return column_entry

    def _pre_extract(self):
        pass

    def _post_extract(self, record):
        """Perform some post extract logic on the extracted record
        In the case of filedb, we want to perform additional actions
        based on metacol definitions
        """

        ins_colname = self._argv.metacols.get(const.METADATA_INSERT_TS_COL)
        if ins_colname:
            record.append(self._now_str)

        upd_colname = self._argv.metacols.get(const.METADATA_UPDATE_TS_COL)
        if upd_colname:
            record.append(self._now_str)

        return record

    def build_extract_data_sql(self, column_list, table, extractlsn,
                               samplerows, lock, query_condition):
        """For file-based extracts, the query string is simply the
        tablename and, by convention, the underlying FileDb will
        search for files with a basename of the tablename.
        For example, if tablename = 'foo', then FileDb match any of the
        following file names (and use the first matching one):
            - foo.bar
            - foo.bar.gz
            - foo.bar.bz2
            - foo.csv
        but will not match the following:
            - foobar.csv
            - foobar.csv.gz
            - foo-bar.csv
            - foo_bar.csv
        """
        self._db.samplerows = samplerows
        return str(table.name)

    def table_exists(self, table):
        return self._db.get_data_filename(table.name) is not None

    def bulk_write(self, **kwargs):
        raise NotSupportedException(
            "bulk_write currently not supported for Files")

    def delete(self, table, query_condition):
        raise NotSupportedException(
            "delete currently not supported for Files")

    def truncate(self, table):
        raise NotSupportedException(
            "truncate currently not supported for Files")

    def drop(self, table, cascade):
        raise NotSupportedException(
            "drop currently not supported for Files")
