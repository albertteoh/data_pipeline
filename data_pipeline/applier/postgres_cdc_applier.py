###############################################################################
# Module:    postgres_applier
# Purpose:   Applies CDCs polled from stream to a Postgres DB
#
# Notes:     This module focuses on logic for applying CDCs, often involving
#            execution of individual SQL statements
#
###############################################################################

import logging
import data_pipeline.constants.const as const

from .postgres_applier import PostgresApplier
from .exceptions import ApplyError


class PostgresCdcApplier(PostgresApplier):
    def __init__(self, source_processor, target_db, argv, audit_factory,
                 sql_builder):
        super(PostgresCdcApplier, self).__init__(
            source_processor, target_db, argv, audit_factory, sql_builder)
        self._logger = logging.getLogger(__name__)

    def _execute_statement(self, statement, commit_lsn):
        if self._can_buffer(statement):
            if self._bulk_ops.max_count == self._argv.bulkinsertlimit:
                self._logger.debug("Bulk apply insert limit hit: {c}. "
                                   "Executing bulk insert..."
                                   .format(c=self._bulk_ops.max_count))

                self._execute_bulk_ops()

            self._bulk_ops.add(statement,
                               commit_lsn,
                               self.current_message_offset)

            self.recovery_offset = self._bulk_ops.start_offset

            self._logger.debug(
                "Added statement {s}. Total count = {c}"
                .format(s=statement,
                        c=len(self._bulk_ops[statement.table_name])))
        else:
            # We've received a type other than insert, so we'll flush
            # all the accumulated insert statements out
            self._execute_bulk_ops()

            # Then execute the non-insert statement individually
            self.recovery_offset = self.current_message_offset
            sql = statement.tosql(self)

            if not sql:
                self._logger.warn(
                    "No resulting SQL string built from statement: "
                    "{statement}. Not executing..."
                    .format(statement=statement))
            else:
                self._execute_sql(sql, commit_lsn, self.current_message_offset)

    def _commit_statements(self):
        self._target_db.commit()
        self._logger.debug("Batch committed")

        self._output_file.write("{};\n".format(const.COMMIT))
        self._output_file.flush()
