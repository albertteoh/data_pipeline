###############################################################################
# Module:    greenplum_applier
# Purpose:   Applies CDCs polled from stream to a Greenplum DB
#
# Notes:     Greenplum is essentially a Postgres DB, hence why it inherits from
#            PostgresCdcApplier. The only differentiator is the need to remove
#            primary keys from the SET clause of an UPDATE statement.
#
###############################################################################


import logging

from .postgres_cdc_applier import PostgresCdcApplier


class GreenplumCdcApplier(PostgresCdcApplier):
    def __init__(self, source_processor, target_db, argv, audit_factory,
                 sql_builder):
        super(GreenplumCdcApplier, self).__init__(
            source_processor, target_db, argv, audit_factory, sql_builder)
        self._logger = logging.getLogger(__name__)
