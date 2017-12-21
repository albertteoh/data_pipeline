###############################################################################
# Module:    table_name
# Purpose:   Represents a table name containing the schema and name of table
#
# Notes:
#
###############################################################################


class TableName(object):
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name

        self.fullname = ".".join(filter(lambda x: x,
                                        [schema, name]))

    def __str__(self):
        return self.fullname
