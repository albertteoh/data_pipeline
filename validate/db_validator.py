##################################################################################
# Module:    db_validator.py
# Purpose:   Databse validator
#
# Notes:
#
##################################################################################


import datetime
import logging
import utils.args as args
import utils.validate_orm as validate_orm
import utils.db_data_dictionary as db_data_dictionary
import utils.db_sql_runner as db_sql_runner
import data_pipeline.logger.logging_loader as logging_loader
import constants.validate_const as validate_const


# Globals
argv = args.get_program_args()


def computeVerifyDetails(conn_audit, conn_db_raw, validate_tag):
    validatedtl = validate_orm.getValidateDetail(conn_audit)

    select_statement = validatedtl.select(validatedtl.c.validate_tag == validate_tag)
    result_set = conn_audit.execute(select_statement)

    for row in result_set:
        where_clause = "1=1"

        if row['predicate_meta']:
            where_clause = row['predicate_meta']

        if row['validate_method'] == validate_const.VALIDATE_METHOD_ROWCOUNT:
            do_sql = "select count(*) from {}.{} where {}".format(row['object_schema'], row['object_name'], where_clause)
            val = db_sql_runner.runSQLSimpleFetch(conn_db_raw, do_sql, argv.arraysize)

        elif row['validate_method'] == validate_const.VALIDATE_METHOD_META:
            do_sql = "select {} from {}.{} where {}".format(row['validate_meta'], row['object_schema'], row['object_name'], where_clause)
            val = db_sql_runner.runSQLSimpleFetch(conn_db_raw, do_sql, argv.arraysize)

        else:
            raise ValueError('Validation mode "{}" not implemented'.format(row['validate_method']))

        update_stmt = validatedtl.update().where(validatedtl.c.validate_detail_id == row['validate_detail_id']).values(
            validate_int=val)
        conn_audit.execute(update_stmt)
        update_stmt = validatedtl.update().where(validatedtl.c.validate_detail_id == row['validate_detail_id']).values(
            validate_time=datetime.datetime.now())
        conn_audit.execute(update_stmt)

def populateVerifyDetails(conn_audit, conn_db_raw, validate_tag):
    validate = validate_orm.getValidate(conn_audit)
    validatedtl = validate_orm.getValidateDetail(conn_audit)

    select_statement = validate.select(validate.c.validate_tag == validate_tag)
    result_set = conn_audit.execute(select_statement)

    for metadata_query_filter in result_set:

        # DB specific query to find tables matching search mask
        sql = db_data_dictionary.getSQLForTableList(conn_db_raw, metadata_query_filter[1], metadata_query_filter[2])
        results = db_sql_runner.runSQLAllFetch(conn_db_raw, sql, argv.arraysize)

        # for every table found matching the search mask, add a row to the ValidateDetail table in the audit database
        for i in results:
            conn_audit.execute(validatedtl.insert()
                               , validate_tag=validate_tag
                               , validate_method=validate_const.VALIDATE_METHOD_ROWCOUNT
                               , object_schema=(i[0]).upper()
                               , object_name=(i[1]).upper()
                               )





def main():
    logging_loader.setup_logging(argv.workdirectory)
    logger = logging.getLogger(__name__)
    
    conn_audit = db_sql_runner.get_audit_db(argv)
    conn_raw = db_sql_runner.get_raw_db(argv)
    
    if validate_orm.isVerifyDetailPopulated(conn_audit, argv.validatetag):
        # Have found matching entries in verify_detail; now process them
        logger.info('Found data for tag "{}"'.format(argv.validatetag))
        computeVerifyDetails(conn_audit, conn_raw, argv.validatetag)

    else:
        # No matching entries in verify_detail; need to add them
        logger.info('No data found data for tag "{}"'.format(argv.validatetag))
        populateVerifyDetails(conn_audit, conn_raw, argv.validatetag)
        computeVerifyDetails(conn_audit, conn_raw, argv.validatetag)



if __name__ == "__main__":
    main()    

