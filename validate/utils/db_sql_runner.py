##################################################################################
# Module:   db_sql_runner
# Purpose:  Basic SQL helper routines
#
# Notes:
#
##################################################################################

import data_pipeline.utils.dbuser as dbuser
import data_pipeline.db.factory as db_factory
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)


def get_raw_db(argv):
    conn_details = dbuser.get_dbuser_properties(argv.validateuser)
    con = db_factory.build(argv.validatedbtype)
    con.connect(conn_details)
    return con

def get_audit_db(argv):
    connection_string = 'postgresql+psycopg2://{}'.format(argv.audituser)
    engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
    # engine = create_engine('sqlite:///local_sqllite.db')
    return engine

def runSQLSimpleFetch(con, sql, arraysize):
    rs = con.execute_query(sql, arraysize)
    val=""
    for r in rs:
        val = r[0]
    logger.debug('runSQLSimpleFetch "{}" returned "{}"'.format(sql, val))
    return (val)


def runSQLAllFetch(con, sql, arraysize):
    logger.debug('runSQLAllFetch "{}"'.format(sql))
    rs = con.execute_query(sql, arraysize)
    results = rs.fetchall()
    return results
