###############################################################################
# Module:    sqldb
# Purpose:   Represents an abstract SQL database
#
# Notes:
#
###############################################################################


import data_pipeline.sql.builder.utils as sql_utils
import data_pipeline.sql.builder.factory as sql_builder_factory
import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod

from data_pipeline.sql.statement.create_statement import CreateStatement
from initsyncdb import InitSyncDb


# ASCII chars to strip from initsync
STRIP_ASCII_CHARCODES = [
    const.ASCII_NULL,
    const.ASCII_STARTOFTEXT,
    const.ASCII_NEWLINE,
    const.ASCII_CARRIAGERETURN,
    const.ASCII_GROUPSEPARATOR,
    const.ASCII_RECORDSEPARATOR
]


class SqlDb(InitSyncDb):
    def __init__(self, argv, db, logger):
        super(SqlDb, self).__init__(argv, db, logger)
        self._sql_builder = None

    @property
    def sql_builder(self):
        # Delay creation of sql_builder so argv.datatypemap is not
        # referenced until needed. Means users don't have to set this
        # configuration if not creating tables or using gpload
        if self._sql_builder is None:
            self._sql_builder = sql_builder_factory.build(
                self._db.dbtype, self._argv
            )

        return self._sql_builder

    def query_columns(self, table, lowercase):
        colname_sql = self._build_colname_sql(table, lowercase)
        results = self._db.execute_query(colname_sql, const.DEFAULT_ARRAYSIZE)
        return map(self._get_column_attributes, results)

    def _get_column_attributes(self, row):
        # Currently, we assume a max number of params to be 2
        MAX_PARAMS_LENGTH = 2

        COLNAME_INDEX = 0
        DATATYPE_INDEX = 1
        LENGTH_INDEX = 2

        colname = row[COLNAME_INDEX]
        datatype = row[DATATYPE_INDEX]

        # We only want the size param (index == 2) for string data types
        if self._is_string_type(datatype) and row[LENGTH_INDEX]:
            params = [str(row[LENGTH_INDEX])]
        else:
            # Otherwise we want everything after the length index
            params = [str(p) for p in row[(LENGTH_INDEX+1):] if p]

        print("get_column_attributes=({}, {}, {})".format(colname, datatype, params))
        return (colname, datatype, params)

    def query_keycolumnlist(self, table):
        COLNAME_INDEX = 1

        keycolumnlist_sql = self.sql_builder.build_keycolumnlist_sql(
            [table.schema], [table.name])
        result = self._db.execute_query(keycolumnlist_sql, const.DEFAULT_ARRAYSIZE)
        return [r[COLNAME_INDEX] for r in result]

    def get_decorated_source_column_list(
            self,
            column_list_query_result,
            definition_origin):
        column_list = []
        for row in column_list_query_result:
            (colname, datatype, params) = row

            if definition_origin == const.TARGET:
                # Remove trailing underscore from column names that are
                # reserved words. Only do this if column defs are from target
                colname_stripped_underscore = colname.rstrip(const.UNDERSCORE)
                if colname_stripped_underscore.upper() in const.RESERVED_SQL_WORDS:
                    colname = colname_stripped_underscore

            # Wrap colname to handle special characters in name
            # Why not make this default behaviour? It's because wrapping
            # enforces case sensitivity and if the loaddefinition is set
            # to destination, column case could be different causing the
            # select statement to fail.
            if self._argv.nonstandardcolumnnames:
                colname = self._wrap_colname(colname)
            colname = self._add_column_modifiers(colname, datatype)
            column_entry = {
                const.FIELD_NAME: colname,
                const.DATA_TYPE: datatype,
                const.PARAMS: params,
            }

            column_list.append(column_entry)

        return column_list

    def get_decorated_target_column_list(
            self,
            column_list_query_result,
            definition_origin):
        column_list = []
        for row in column_list_query_result:
            (colname, datatype, params) = row

            if definition_origin == const.SOURCE:
                # Append an underscore to column names that are
                # reserved words. Only do this if column defs are from source
                if colname.upper() in const.RESERVED_SQL_WORDS:
                    colname += const.UNDERSCORE

            colname = sql_utils.replace_special_chars(
                colname=colname,
                replacement_char=const.SPECIAL_CHAR_REPLACEMENT)

            column_entry = {
                const.FIELD_NAME: colname,
                const.DATA_TYPE: datatype,
                const.PARAMS: params,
            }

            column_list.append(column_entry)

        return column_list

    def _build_ignore_columns_sql(self, column_name_name):
        ignore_cols_sql = const.EMPTY_STRING
        if self._ignored_columns:
            ignore_cols_sql = (
                "AND {colname} NOT IN ({ignored_list})"
                .format(colname=column_name_name,
                        ignored_list=const.COMMASPACE.join(
                            ["'{}'".format(x) for x in self._ignored_columns]
                        )
                )
            )
        return ignore_cols_sql

    def _build_colname_select(self, lowercase, column_name_name):
        colname_select = column_name_name
        if lowercase:
            colname_select = ("LOWER({colname}) AS {colname}"
                              .format(colname=colname_select))
        return colname_select

    @abstractmethod
    def _build_colname_sql(self, table, lowercase):
        pass

    def _add_column_modifiers(self, colname, datatype):
        # Augment string values to make them suitable for applying to target
        if self._is_string_type(datatype):
            colname = self._pre_wrap_with_ascii_replace(colname, datatype)

            copy_of_charcodes = STRIP_ASCII_CHARCODES[:]
            colname = self._wrap_with_ascii_replace(copy_of_charcodes, colname)

            colname = self._post_wrap_with_ascii_replace(colname, datatype)

        # Defer handling of other datatypes to the child db types
        else:
            colname = self._add_column_modifiers_for_other_datatypes(colname,
                                                                     datatype)

        return colname

    @abstractmethod
    def _wrap_colname(self, colname):
        """Wraps the given column name with bounding characters to handle
        special characters within the column name
        """
        pass

    @abstractmethod
    def _pre_wrap_with_ascii_replace(self, colname, datatype):
        """Prepares the string-typed colname prior to wrapping it with
        REPLACE functions. This preparation is deferred to the database-specific
        implementation and, more often than not, nothing needs to be done
        """
        pass

    @abstractmethod
    def _post_wrap_with_ascii_replace(self, colsql, datatype):
        """Decorates the string-typed colname after wrapping it with
        REPLACE functions. This decoration is deferred to the database-specific
        implementation and, more often than not, nothing needs to be done
        """
        pass

    def _wrap_with_ascii_replace(self, ascii_list, body):
        if not ascii_list:
            return body

        ascii_char_num = ascii_list.pop()
        while not self._is_valid_char_num(ascii_char_num):
            if not ascii_list:
                return body
            ascii_char_num = ascii_list.pop()

        body = ("REPLACE({body}, {ascii_function}({num}), '')"
                .format(body=body,
                        ascii_function=self._get_ascii_function(),
                        num=ascii_char_num))

        return self._wrap_with_ascii_replace(ascii_list, body)

    @abstractmethod
    def _is_valid_char_num(self, char_num):
        pass

    @abstractmethod
    def _get_ascii_function(self):
        pass

    @abstractmethod
    def _is_string_type(self, datatype):
        pass

    @abstractmethod
    def _add_column_modifiers_for_other_datatypes(self, colname, datatype):
        pass

    def _build_metacols_sql(self):
        metacols_sql = ""

        if self._argv.metacols:
            ins_colname = self._argv.metacols.get(const.METADATA_INSERT_TS_COL)
            if ins_colname:
                metacols_sql += """
            , CURRENT_TIMESTAMP AS INS_TIMESTAMP -- insert timestamp"""

            upd_colname = self._argv.metacols.get(const.METADATA_UPDATE_TS_COL)
            if upd_colname:
                metacols_sql += """
            , CURRENT_TIMESTAMP AS UPD_TIMESTAMP -- update timestamp"""

        return metacols_sql

    @abstractmethod
    def build_extract_data_sql(self, column_list, table, samplerows, lock):
        pass

    @abstractmethod
    def _pre_extract(self):
        pass

    def _post_extract(self, record):
        return record

    def _append_query_condition(self, sql, query_condition, table):
        if query_condition:
            sql += """
          AND {query_condition}""".format(query_condition=query_condition)

            # Support some reserved tokens in the query
            sql = sql.format(
                SCHEMA=table.schema,
                TABLE=table.name,
            )

        return sql

    def delete(self, table, query_condition):
        sql = self._build_delete_sql(table, query_condition)
        return self._db.execute(sql)

    def _build_delete_sql(self, table, query_condition):
        sql = """
        DELETE FROM {table}
        WHERE 1=1""".format(table=table.fullname)

        sql = self._append_query_condition(sql, query_condition, table)
        return sql

    def truncate(self, table):
        sql = self._build_truncate_sql(table)
        return self._db.execute(sql)

    @abstractmethod
    def _build_truncate_sql(self, table):
        pass

    def drop(self, table, cascade):
        sql = self._build_drop_sql(table, cascade)
        return self._db.execute(sql)

    def _build_drop_sql(self, table, cascade):
        cascade_sql = const.EMPTY_STRING
        if cascade:
            cascade_sql = " CASCADE"

        sql = "DROP TABLE IF EXISTS {full_table_name}{cascade}".format(
                  full_table_name=table.fullname,
                  cascade=cascade_sql,
              )
        return sql

    def create(self, table, column_list, keycolumnlist):
        sql = self._build_create_sql(table, column_list, keycolumnlist)
        return self._db.execute(sql)

    def _build_create_sql(self, table, column_list, keycolumnlist):
        create_statement = CreateStatement(table.name)
        for column_record in column_list:
            create_statement.add_entry(
                field_name=column_record[const.FIELD_NAME],
                data_type=column_record[const.DATA_TYPE],
                params=column_record[const.PARAMS],
            )

        if keycolumnlist:
            create_statement.add_entry(keys=keycolumnlist)

        sql = self.sql_builder.build_create_sql(create_statement)

        return sql

    def vacuum(self, table):
        sql = self._build_vacuum_sql(table)
        self._logger.info("Executing: {sql}".format(sql=sql))
        self._db.execute(sql)

    @abstractmethod
    def _build_vacuum_sql(self, table):
        pass

    def analyze(self, table):
        sql = self._build_analyze_sql(table)
        self._logger.info("Executing: {sql}".format(sql=sql))
        self._db.execute(sql)

    @abstractmethod
    def _build_analyze_sql(self, table):
        pass
