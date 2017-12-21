##################################################################################
# Module:    validate_orm.py
# Purpose:   Databse validator ORM objects
#
# Notes:
#
##################################################################################

from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime



def getValidate(conn_audit):
    meta = MetaData(conn_audit)
    validate = Table('validate', meta,
        Column('validate_tag', String, primary_key=True),
        Column('object_schema_regexp', String),
        Column('object_name_regexp', String)
        )
    return validate

def getValidateDetail(conn_audit):
    meta = MetaData(conn_audit)
    validatedtl = Table('validate_detail', meta,
        Column('validate_detail_id', Integer, primary_key=True),
        Column('validate_tag', String),
        Column('validate_method', String),
        Column('object_schema', String),
        Column('object_name', String),
        Column('validate_int', Integer),
        Column('validate_time', DateTime),
        Column('validate_meta', String),
        Column('predicate_meta', String)
        )
    return validatedtl

def isVerifyDetailPopulated(conn_audit, validate_tag):
    validatedtl = getValidateDetail(conn_audit)
    select_statement = validatedtl.select(validatedtl.c.validate_tag == validate_tag)
    result_set = conn_audit.execute(select_statement)
    rows = result_set.fetchall()
    if len(rows):
        return True
    else:
        return False