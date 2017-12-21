###############################################################################
# Module:  factory
# Purpose: Build concrete instances of specific sql_builders
#
# Notes:
#
###############################################################################


import importlib


def build(dbtype, argv):
    module_name = ("data_pipeline.sql.builder.{type}_sql_builder"
                   .format(type=dbtype))
    module = importlib.import_module(module_name)

    class_name = "{upper}{rest}SqlBuilder".format(
        upper=dbtype[0].upper(),
        rest=dbtype[1:],
    )
    constructor = getattr(module, class_name)
    return constructor(argv)
