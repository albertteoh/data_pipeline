###############################################################################
# Module:    postgres_applier
# Purpose:   Applies data acquired from stream to a Postgres DB
#
# Notes:     This module concerns itself mainly with generating Postgres SQL
#            for applying to target and mapping data types.
#
###############################################################################

import sys
import logging
import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod
from .applier import Applier
from .exceptions import ApplyError


class PostgresApplier(Applier):
    __metaclass__ = ABCMeta

    def __init__(self, source_processor, target_db,
                 argv, audit_factory, sql_builder):
        super(PostgresApplier, self).__init__(
            const.CDCAPPLY, source_processor, target_db,
            argv, audit_factory, sql_builder)

    @abstractmethod
    def _execute_statement(self, statement):
        """Execute statement on target
        :param Statement statement: The statement to execute on target
        """
        pass

    @abstractmethod
    def _commit_statements(self):
        """Commit all executed statements/transactions on target
        """
        pass
