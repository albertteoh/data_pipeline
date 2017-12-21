###############################################################################
# Module:    sql_builder
# Purpose:   Builds generic sql statements for databases
#
# Notes:
#
###############################################################################


import data_pipeline.sql.builder.utils as utils
import yaml
import data_pipeline.constants.const as const

from abc import ABCMeta, abstractmethod


def _map_operation(operation):
    if operation == const.MODIFY:
        return const.ALTER
    return operation


def _build_ddl_entry(entry, datatype_mapper):
    """Builds a DDL entry string. For example: MYFIELD VARCHAR(400)
    :param dict entry: A dict of DDL entry details
    :param Applier datatype_mapper: An applier for the specific target
        which knows how to map from a generic datatype to target datatype
    """
    datatype = utils.build_datatype_sql(entry[const.DATA_TYPE],
                                        entry[const.PARAMS],
                                        datatype_mapper)

    constraints = utils.build_field_string(
        entry.setdefault(const.CONSTRAINTS,
                         const.EMPTY_STRING))

    type_modifier = const.EMPTY_STRING
    if entry[const.OPERATION] == const.MODIFY:
        type_modifier = " {t}".format(t=const.TYPE)

    field_name = utils.build_field_string(entry[const.FIELD_NAME])
    return ("{field_name}{type_modifier}{data_type}{constraints}"
            .format(field_name=field_name,
                    type_modifier=type_modifier,
                    data_type=datatype,
                    constraints=constraints)).strip()


class SqlBuilder(object):
    __metaclass__ = ABCMeta

    def __init__(self, argv):
        self._argv = argv
        self._config = None
        self._update_field_filter_func = utils.default_update_field_filter

    @abstractmethod
    def build_keycolumnlist_sql(self):
        pass

    def build_insert_sql(self, insert_statement):
        return utils.build_insert_sql(insert_statement, self._argv.targetschema)

    def build_update_sql(self, update_statement):
        return utils.build_update_sql(
            update_statement, schema=self._argv.targetschema,
            filter_func=self._update_field_filter_func)

    def build_delete_sql(self, delete_statement):
        return utils.build_delete_sql(delete_statement, self._argv.targetschema)

    def build_alter_sql(self, alter_statement):
        return utils.build_alter_sql(alter_statement,
                                     _map_operation,
                                     _build_ddl_entry,
                                     self,
                                     self._argv.targetschema)

    def build_create_sql(self, create_statement):
        return utils.build_create_sql(create_statement, self,
                                      self._argv.targetschema)

    def _get_target_int_datatype(self, datatype_config, source_precision):
        """Maps the source integer precision to the equivalent
           target integer data type
        """
        rules = datatype_config.get(const.RULES, list())
        for rule in rules:
            precision_config = rule.get(const.PRECISION, None)
            if precision_config is not None:
                p_start = int(precision_config[const.PRECISION_START])
                p_end = int(precision_config[const.PRECISION_END])

                if source_precision >= p_start and source_precision <= p_end:
                    target_datatype = rule.get(const.TARGET, None)
                    keep_params = rule.get(const.KEEP_PARAMS, None)
                    return (target_datatype, keep_params)
        return None

    def get_target_datatype(self, datatype, params):
        if not datatype:
            return const.EMPTY_STRING

        target_datatype = datatype
        target_params = params

        # This check shouldn't be necessary as we should only be calling
        # this function if loaddefinition is SOURCE
        if self._argv.loaddefinition != const.TARGET:
            datatype_config = self._get_datatype_config(
                self._get_source_target_config(),
                datatype,
            )

            # If there is a mapping defined
            if datatype_config != const.PASS:
                self._logger.debug("Found mapping definition for {} -> {}"
                                   .format(datatype, datatype_config))

                target_datatype = datatype_config.get(const.TARGET, None)
                keep_params = datatype_config.get(const.KEEP_PARAMS, True)

                source_precision = None
                source_scale = 0

                if params:
                    if len(params) > 0:
                        source_precision = int(params[const.PRECISION_PARAM_INDEX])
                    if len(params) > 1:
                        source_scale = int(params[const.SCALE_PARAM_INDEX])

                if source_scale == 0:
                    tup = self._get_target_int_datatype(datatype_config,
                                                        source_precision)
                    if tup is not None:
                        (target_datatype, keep_params) = tup

                # Last resort default
                if target_datatype is None:
                    self._logger.warn("No target datatype was configured "
                                      "for source datatype: {}. Defaulting to {}."
                                      .format(datatype, const.DEFAULT_DATATYPE))
                    target_datatype = const.DEFAULT_DATATYPE

                target_params = None
                if keep_params:
                    target_params = params

                self._logger.debug("Mapping {}({}) -> {}({})"
                                   .format(datatype, params, target_datatype, target_params))

        target_datatype_sql = "{datatype}{params}".format(
            datatype=utils.build_field_string(target_datatype.upper()),
            params=utils.build_field_params(target_params)
        )

        return target_datatype_sql

    def _get_source_target_config(self):
        if self._config is None:
            if self._argv.datatypemap is None:
                raise Exception(
                    "Please provide datatypemap configuration which is "
                    "required for mapping datatypes between source and target "
                    "systems")

            self._config = yaml.load(file(self._argv.datatypemap))

        source_target_config = self._config.get(self._argv.sourcedbtype, None)
        if source_target_config is None:
            raise Exception("There is no source db type '{}' defined in {}"
                            .format(self._argv.sourcedbtype,
                                    self._argv.datatypemap))
        return source_target_config


    def _get_datatype_config(self, source_target_config, datatype):
        datatype_config = source_target_config.get(datatype.lower(), None)

        if datatype_config is None:
            raise Exception("There is no {} data type '{}' defined in {}"
                             .format(self._argv.sourcedbtype,
                                     datatype.lower(), self._argv.datatypemap))
        return datatype_config
