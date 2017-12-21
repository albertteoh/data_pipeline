###############################################################################
# Module:    initsync_db
# Purpose:   Contains database endpoint specific functions for initsync
#
# Notes:     This module will route calls to the module responsible for the
#            concrete implementation of functionality.
#
###############################################################################

import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod, abstractproperty


class InitSyncDb(object):
    __metaclass__ = ABCMeta

    def __init__(self, argv, db, logger):
        self._argv = argv
        self._db = db
        self._logger = logger
        self._ignored_columns = None
        if self._argv.metacols:
            self._ignored_columns = self._argv.metacols.get(
                const.METADATA_IGNORED_COLS)

    @property
    def dbtype(self):
        return self._db.dbtype

    @property
    def encoding(self):
        return self._db.encoding

    @abstractmethod
    def query_columns(self, table, lowercase):
        pass

    @abstractmethod
    def get_decorated_source_column_list(self, query_result):
        pass

    @abstractmethod
    def get_decorated_target_column_list(self, query_result):
        pass

    def _get_colnames_from_column_list(self, column_list):
        """Gets the "column names" from the column_list
        The column_list is a list of dictionaries containing the column name,
        column data type, and any associated parameters to the data type such
        as size (for varchar) or scale and precision (for numeric types).
        """
        return [r[const.FIELD_NAME] for r in column_list]

    @abstractmethod
    def table_exists(self, table):
        pass

    @abstractmethod
    def bulk_write(self, **kwargs):
        pass

    def connect(self, connection_details):
        self._db.connect(connection_details)

    def commit(self):
        self._db.commit()

    @abstractmethod
    def delete(self, table, query_condition):
        pass

    @abstractmethod
    def truncate(self, table):
        pass

    @abstractmethod
    def drop(self, table, cascade):
        pass

    def extract_data(self, column_list, table, query_condition, log_function):
        self._pre_extract()

        extract_data_sql = self.build_extract_data_sql(
            column_list, table, self._argv.extractlsn, self._argv.samplerows,
            self._argv.lock, query_condition)

        log_function(self._argv, table, extract_data_sql)

        return self._db.execute_query(extract_data_sql,
                                      self._argv.arraysize,
                                      post_process_func=self._post_extract)

    @abstractmethod
    def _pre_extract(self):
        """This will be run prior to the extract_data sql query, useful for
        setting up session-specific configuration.
        """
        pass

    @abstractmethod
    def _post_extract(self, record):
        """This will be run on a per-record basis to modify data at runtime
        as necessary to allow for any custom data manipulation"""
        pass

    @abstractmethod
    def build_extract_data_sql(self, column_list, table, extractlsn,
                               samplerows, lock, query_condition):
        pass

    def close(self):
        self._db.close()
