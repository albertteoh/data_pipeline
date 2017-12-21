###############################################################################
# Module:    factory
# Purpose:   Builds the initsync endpoint modules for specific databases
#
# Notes:
#
###############################################################################

import importlib
import data_pipeline.db.factory as db_factory
import data_pipeline.constants.const as const


def build(dbtype_name, argv, logger, sourceortarget):
    db = db_factory.build(dbtype_name)

    module_name = "data_pipeline.initsync.{type}db".format(type=dbtype_name)
    module = importlib.import_module(module_name)

    class_name = "{upper}{rest}Db".format(upper=dbtype_name[0].upper(),
                                          rest=dbtype_name[1:])
    constructor = getattr(module, class_name)

    if dbtype_name == const.FILE:
        delim = argv.sourcedelimiter
        if sourceortarget == const.TARGET:
            delim = argv.targetdelimiter
        return constructor(argv, db, logger, delim, argv.quotechar)

    return constructor(argv, db, logger)
