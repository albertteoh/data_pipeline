###############################################################################
# Module:  factory
# Purpose: Build concrete instances of specific appliers
#
# Notes:
#
###############################################################################


import data_pipeline.constants.const as const
from data_pipeline.db.exceptions import UnsupportedDbTypeError


def build(source_processor, db, argv, audit_factory):
    """Return the specific type of applier object given the dbtype_name"""
    if db.dbtype == const.POSTGRES:
        from data_pipeline.applier.postgres_cdc_applier import (
            PostgresCdcApplier
        )
        from data_pipeline.sql.builder.postgres_sql_builder import (
            PostgresSqlBuilder
        )
        return PostgresCdcApplier(source_processor, db, argv, audit_factory,
                                  PostgresSqlBuilder(argv))
    elif db.dbtype == const.GREENPLUM:
        from data_pipeline.applier.greenplum_cdc_applier import (
            GreenplumCdcApplier
        )
        from data_pipeline.sql.builder.greenplum_sql_builder import (
            GreenplumSqlBuilder
        )
        return GreenplumCdcApplier(source_processor, db, argv, audit_factory,
                                  GreenplumSqlBuilder(argv))
    else:
        raise UnsupportedDbTypeError(db.dbtype)
