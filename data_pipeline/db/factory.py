###############################################################################
# Module:  factory
# Purpose: Build concrete instances of specific database clients
#
# Notes:
#
###############################################################################

import importlib
import data_pipeline.constants.const as const


def build(dbtype_name):
    """Return the specific type of db object given the dbtype_name"""

    module_name = "data_pipeline.db.{type}db".format(type=dbtype_name)
    module = importlib.import_module(module_name)

    class_name = "{upper}{rest}Db".format(upper=dbtype_name[0].upper(),
                                          rest=dbtype_name[1:])
    constructor = getattr(module, class_name)

    return constructor()
