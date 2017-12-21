###############################################################################
# Module:   initsync_pipe
# Purpose:  A module that performs an initial sync of data from source to
#           target databases using unix fifo pipes.
#
# Notes:    A non object oriented approach was taken here due to the added
#           complexity introduced by classes, particularly relating to sharing
#           state within the object between processes as well as issues with
#           serialisation which appears to be a large pain-point when using
#           multiproc, according to some online literature.
#           As such, the emphasis was placed on functions with explicit passing
#           of arguments and using message queues for passing state between
#           processes; instead of stateful global variables.
#
#           pathos was used primarily to overcome limitations of the built-in
#           multiprocessing module in order to pass more than one argument to
#           the target function. It's apparently also better at serialising
#           stuff.
#
###############################################################################

import confluent_kafka
import datetime
import errno
import logging
import multiprocessing
import os
import re
import shlex
import stat
import subprocess
import sys
import time

import initsync.factory as db_factory
import data_pipeline.constants.const as const
import data_pipeline.logger.logging_loader as logging_loader
import data_pipeline.utils.dbuser as dbuser
import data_pipeline.utils.mailer as mailer

from pathos.multiprocessing import ProcessingPool as Pool

from data_pipeline.stream.factory import (build_file_reader,
                                          build_file_writer,
                                          build_kafka_consumer)
from data_pipeline.common import get_program_args, log_version
from data_pipeline.sql.table_name import TableName
from data_pipeline.audit.factory import get_audit_db
from data_pipeline.audit.custom_orm import (ProcessControl,
                                            ProcessControlDetail,
                                            SourceSystemProfile)


manager = multiprocessing.Manager()
row_count_re = re.compile(r"(\d+) rows copied\.")


class InitSyncKafkaClient(object):
    """A very basic Kafka Client specific to InitSync whose purpose
    is to simply expose the process_control used by initsync"""
    def __init__(self, process_control, offset):
        self.process_control = process_control
        self.next_offset_to_read = offset

    def report_error(self, error_message):
        pass


def parallelise_initsync(argv, ssp_params,
                         process_control_id, logger):
    # Pivot the collection of source_system_profile records into
    # three separate lists to enable us to call pool.map on each record
    (source_schemas, tables, target_schemas, query_conditions) = map(
        list,
        zip(*ssp_params))

    source_conn_detail = dbuser.get_dbuser_properties(argv.sourceuser)
    target_conn_detail = dbuser.get_dbuser_properties(argv.targetuser)

    logger.info("Processing tables with {} dedicated worker processes"
                .format(argv.numprocesses))
    pool = Pool(nodes=argv.numprocesses)

    argvs = [argv] * len(tables)
    source_conn_details = [source_conn_detail] * len(tables)
    target_conn_details = [target_conn_detail] * len(tables)
    pcids = [process_control_id] * len(tables)
    queues = [manager.Queue()] * len(tables)

    logger.debug("Starting a new process for each table in: {tables}"
                 .format(tables=tables))
    # Execute initsync for each schema/table combination in parallel
    pool.map(initsync_table,
             argvs,
             source_conn_details,
             target_conn_details,
             source_schemas,
             tables,
             target_schemas,
             pcids,
             query_conditions,
             queues,
             # Ensure tables processed in sequence and workers fully utilised
             chunksize=1)

    pool.close()
    logger.debug("parallelise_initsync: Pool joining")
    pool.join()
    logger.debug("parallelise_initsync: Pool joined")

    all_table_results = {}
    for q in queues:
        size = q.qsize()
        message = q.get()
        logger.debug("Message queue size = {s}, message = {m}"
                     .format(s=size, m=message))
        all_table_results.update(message)

    logger.debug("all_table_results = {r}".format(r=all_table_results))
    return all_table_results


def put_table_result(queue, table_name, lsn, status, process_code, msg):
    result = {}
    result[table_name] = (lsn, status, process_code, msg)
    queue.put(result)


def initsync_table(argv, source_conn_details, target_conn_details,
                   source_schema, table_name, target_schema,
                   process_control_id, query_condition, initsync_msg_queue):

    logging_loader.setup_logging(argv.workdirectory, table_name)
    logger = logging.getLogger(__name__)

    logger.info("{worker} Starting InitSync on table: {table}"
                .format(worker=multiprocessing.current_process(),
                        table=table_name))

    lsn = None
    source_db = None
    target_db = None
    try:
        mode = const.INITSYNC

        _update_source_system_profile(
            argv, source_schema, target_schema,
            table_name, const.IN_PROGRESS, process_control_id, mode)

        pc_detail = ProcessControlDetail(argv, mode, process_control_id)
        pc_detail.insert(
            object_schema=source_schema,
            object_name=table_name,
            comment="Starting InitSync",
            process_code=mode,
            infolog=logging_loader.get_logfile(const.INFO_HANDLER),
            errorlog=logging_loader.get_logfile(const.ERROR_HANDLER)
        )

        source_table = TableName(source_schema, table_name)
        target_table = TableName(target_schema, table_name)

        source_db = get_source_db(argv, source_conn_details, logger)
        target_db = get_target_db(argv, target_conn_details, logger)

        # Check if source table exists
        if not _table_exists(
                argv, source_table, source_db,
                initsync_msg_queue, pc_detail, logger, const.SOURCE):
            return

        logger.debug("Loading column lists from {} DB"
                     .format(argv.loaddefinition))

        (source_column_list,
         target_column_list,
         keycolumnlist) = get_column_lists_by_definition_origin(
            source_db, source_table,
            target_db, target_table,
            definition_origin=argv.loaddefinition)

        logger.debug("\n"
                     "{table} source_column_list:\n"
                     "{sourcecols}\n"
                     "{table} target_column_list:\n"
                     "{targetcols}\n"
                     "{table} keycolumnlist:\n"
                     "{keycols}"
                     .format(table=table_name,
                             sourcecols=source_column_list,
                             targetcols=target_column_list,
                             keycols=keycolumnlist)
        )

        execute_pre_processing(argv, target_db, target_table, query_condition,
                               target_column_list, keycolumnlist,
                               process_control_id, logger)

        # Check if target table exists after any possible
        # drop/create table operations
        if not _table_exists(
                argv, target_table, target_db,
                initsync_msg_queue, pc_detail, logger, const.TARGET):
            return

        pipe_file = create_pipe(argv, source_table, logger)

        applier_proc = None
        extractor_proc = None

        message = ("[{t}] Starting new process for apply..."
                   .format(t=target_table.name))
        logger.debug(message)
        pc_detail.update(
            comment=message
        )

        applier_msg_queue = manager.Queue()
        applier_proc = multiprocessing.Process(
            target=apply,
            args=(argv, pipe_file, target_table, target_column_list,
                  target_db, target_conn_details, process_control_id,
                  query_condition, applier_msg_queue))
        applier_proc.start()
        apply_status = const.IN_PROGRESS

        if not argv.inputfile:
            message = ("[{t}] Starting new process for extract..."
                       .format(t=source_table.name))
            logger.debug(message)
            pc_detail.update(
                comment=message
            )

            extract_msg_queue = manager.Queue()

            extractor_proc = multiprocessing.Process(
                target=extract,
                args=(argv, pipe_file, source_table, source_column_list,
                      source_db, source_conn_details, process_control_id,
                      query_condition, extract_msg_queue))
            extractor_proc.start()
            extract_status = const.IN_PROGRESS
        else:
            extract_status = const.SUCCESS

        status = _manage_child_processes(
            argv, pc_detail,
            extractor_proc, extract_status, extract_msg_queue,
            applier_proc, apply_status, applier_msg_queue,
            source_table, target_table, initsync_msg_queue, logger)

        execute_post_processing(argv, target_table, target_db)

        _update_source_system_profile(
            argv, source_schema, target_schema,
            table_name, status, process_control_id, mode)

    except Exception, e:
        message = ("[{table}] Failed initsync: {err}"
                   .format(table=table_name, err=str(e)))
        logger.exception(message)
        put_table_result(initsync_msg_queue, table_name,
                         lsn, const.ERROR, const.INITSYNC, message)
    finally:
        if source_db:
            source_db.close()
        if target_db:
            target_db.close()


def get_column_lists_by_definition_origin(source_db, source_table,
                                          target_db, target_table,
                                          definition_origin):
    if definition_origin == const.TARGET:
        column_definition_db = target_db
        query_table = target_table
    else:
        column_definition_db = source_db
        query_table = source_table

    keycolumnlist_result = column_definition_db.query_keycolumnlist(
        query_table)

    source_column_query_result = column_definition_db.query_columns(
        query_table, lowercase=False)
    source_column_list = source_db.get_decorated_source_column_list(
        source_column_query_result, definition_origin)

    target_column_query_result = column_definition_db.query_columns(
        query_table, lowercase=True)
    target_column_list = target_db.get_decorated_target_column_list(
        target_column_query_result, definition_origin)

    return (source_column_list, target_column_list, keycolumnlist_result)


def execute_pre_processing(
        argv, target_db, target_table, query_condition,
        column_list, keycolumnlist, process_control_id, logger):

    # gpload does the truncate/delete for us, so don't do it here
    # otherwise, it could lead to a deadlock
    if (argv.delete or argv.truncate) and argv.directload != const.GPLOAD:
        clean_target_table(argv, target_db, target_table, query_condition,
                           process_control_id)

    if argv.droptable or argv.droptablecascade:
        if argv.droptable and argv.droptablecascade:
            logger.warn("Both droptable and droptablecascade requested. "
                        "CASCADE will be executed in preference.")

        drop_target_table(argv, target_db, target_table, process_control_id)

    if argv.createtable:
        create_target_table(argv, target_db, target_table, column_list,
                            keycolumnlist, process_control_id)


def execute_post_processing(argv, target_table, target_db):
    if argv.vacuum:
        target_db.vacuum(target_table)

    if argv.analyze:
        target_db.analyze(target_table)


def drop_target_table(argv, target_db, target_table, process_control_id):
    mode = const.INITSYNCDROP
    comment = "Dropping target table"

    pc_detail = _insert_new_pc_detail(
        argv, process_control_id, mode, comment, target_table)

    target_db.drop(target_table, argv.droptablecascade)

    withcascade = const.EMPTY_STRING
    if argv.droptablecascade:
        withcascade = " with cascade"

    comment = "Dropped {table}{withcascade}".format(
                   table=target_table.fullname,
                   withcascade=withcascade,
              )

    pc_detail.update(
        comment=comment,
        status=const.SUCCESS,
    )


def create_target_table(argv, target_db, target_table, column_list,
                        keycolumnlist, process_control_id):
    mode = const.INITSYNCCREATE
    comment = "Creating target table"

    pc_detail = _insert_new_pc_detail(
        argv, process_control_id, mode, comment, target_table)

    target_db.create(target_table, column_list, keycolumnlist)

    comment = "Created {table}".format(
                   table=target_table.fullname,
              )

    pc_detail.update(
        comment=comment,
        status=const.SUCCESS,
    )


def clean_target_table(argv, target_db, target_table,
                       query_condition, process_control_id):
    mode = const.INITSYNCTRUNC
    comment = "Deleting/Truncating target table"

    pc_detail = _insert_new_pc_detail(
        argv, process_control_id, mode, comment, target_table)

    if argv.delete:
        rowcount = target_db.delete(target_table, query_condition)
    elif argv.truncate:
        rowcount = target_db.truncate(target_table)

    comment = ("Deleted {count} {table} rows"
               .format(count=rowcount, table=target_table.fullname))
    pc_detail.update(
        comment=comment,
        delete_row_count=rowcount,
        source_row_count=rowcount,
        status=const.SUCCESS,
    )


def _insert_new_pc_detail(argv, process_control_id, mode, comment, table):
    pc_detail = ProcessControlDetail(argv, mode, process_control_id)
    pc_detail.insert(
        object_schema=table.schema,
        object_name=table.name,
        comment=comment,
        process_code=mode,
        infolog=logging_loader.get_logfile(const.INFO_HANDLER),
        errorlog=logging_loader.get_logfile(const.ERROR_HANDLER)
    )

    return pc_detail


def _manage_child_processes(
        argv, pc_detail,
        extractor_proc, extract_status, extract_msg_queue,
        applier_proc, apply_status, applier_msg_queue,
        source_table, target_table, initsync_msg_queue, logger):

    extract_msg_timestamp, apply_msg_timestamp = None, None
    extract_status_msg, apply_status_msg = None, None
    lsn = None
    process_code = const.INITSYNC
    try:
        while extract_status == const.IN_PROGRESS:

            # Wait for "heartbeats" from extractor_proc
            extracttimeout = argv.extracttimeout
            if extracttimeout is not None:
                extracttimeout = int(extracttimeout)

            msg = extract_msg_queue.get(True, extracttimeout)

            (extract_msg_timestamp, lsn,
             extract_status, extract_status_msg) = msg

        logger.debug("extract result = {result}".format(result=extract_status))

        # At this point, either an error occurred or extract succeeded
        # Raise an exception on error to prevent applier_proc
        # from blocking indefinitely
        if (extract_status == const.ERROR or extract_status == const.WARNING):
            raise Exception("[{t}] Failed extract".format(t=source_table.name))

        logger.debug("Getting applier result...")
        msg = applier_msg_queue.get(True)
        (apply_msg_timestamp, apply_status, apply_status_msg) = msg
        logger.debug("apply result = {result}".format(result=apply_status))
    except Exception, err:
        logger.warn(
            "[{t}] Failed to get results from extract/apply. "
            "Possibly caused by a timeout during extract. "
            "Consider reducing numprocesses, increasing extracttimeout "
            "or check the source database: {err}"
            .format(t=source_table.name, err=str(err)))

        if apply_status is None:
            apply_status = const.ERROR
        if extract_status is None:
            extract_status = const.ERROR

    if apply_status == const.SUCCESS and extract_status == const.SUCCESS:
        status = const.SUCCESS
        commit_table_lsn(argv, source_table, target_table, lsn)

        if applier_proc:
            logger.debug("[{t}] initsync_table: joining applier_proc"
                         .format(t=target_table))
            applier_proc.join()
            logger.debug("[{t}] Joined applier_proc"
                         .format(t=target_table))

        if extractor_proc:
            logger.debug("[{t}] initsync_table: joining extractor_proc"
                         .format(t=source_table))
            extractor_proc.join()
            logger.debug("[{t}] Joined extractor_proc"
                         .format(t=source_table))

        message = ("Finished initsync from {source_table} "
                   "to {target_table}".format(
                       source_table=source_table,
                       target_table=target_table))
    else:
        status = const.ERROR
        if applier_proc:
            logger.debug("[{t}] initsync_table: terminating applier_proc"
                         .format(t=target_table))
            applier_proc.terminate()
            logger.debug("[{t}] Terminated applier_proc"
                         .format(t=target_table))

        if extractor_proc:
            logger.debug("[{t}] initsync_table: terminating extractor_proc"
                         .format(t=source_table))
            extractor_proc.terminate()
            logger.debug("[{t}] Terminated extractor_proc"
                         .format(t=source_table))

        (process_code, message) = _get_error_process_code_and_msg(
            extract_msg_timestamp, extract_status, extract_status_msg,
            apply_msg_timestamp, apply_status, apply_status_msg)

    pc_detail.update(
        comment=message,
        status=status
    )
    put_table_result(initsync_msg_queue, source_table.name,
                     lsn, status, process_code, message)

    return status


def _get_error_process_code_and_msg(
        extract_msg_timestamp, extract_status, extract_status_msg,
        apply_msg_timestamp, apply_status, apply_status_msg):

    process_code = const.INITSYNC
    message = const.EMPTY_STRING
    if extract_status == const.ERROR and apply_status == const.ERROR:
        if (extract_msg_timestamp is not None and
                apply_msg_timestamp is not None):
            if extract_msg_timestamp < apply_msg_timestamp:
                return (const.INITSYNCEXTRACT, extract_status_msg)
            else:
                return (const.INITSYNCAPPLY, apply_status_msg)
        elif extract_msg_timestamp is not None:
            return (const.INITSYNCEXTRACT, extract_status_msg)
        elif apply_msg_timestamp is not None:
            return (const.INITSYNCAPPLY, apply_status_msg)

    elif extract_status == const.ERROR:
        return (const.INITSYNCEXTRACT, extract_status_msg)

    elif apply_status == const.ERROR:
        return (const.INITSYNCAPPLY, apply_status_msg)

    return (const.INITSYNC, "Cause of error unknown")


def _update_source_system_profile(
        argv, source_schema, target_schema,
        table_name, status, process_control_id, mode):

    source_system_profile = SourceSystemProfile(argv)
    selected = source_system_profile.select(
        profile_name=argv.profilename,
        version=argv.profileversion,
        source_region=source_schema,
        target_region=target_schema,
        object_name=table_name
    )

    if selected:
        source_system_profile.update(
            last_status=status,
            last_run_id=process_control_id,
            last_process_code=mode,
            last_updated=datetime.datetime.now()
        )


def _table_exists(argv, table, db, msg_queue, pc_detail,
                  logger, source_or_target):

    if not db.table_exists(table):
        message = ("Unknown table '{t}' on {source_or_target}"
                   .format(t=table, source_or_target=source_or_target))
        put_table_result(msg_queue, table.name, None,
                         const.WARNING, const.INITSYNC, message)
        logger.warn(message)
        pc_detail.update(
            comment=message,
            status=const.WARNING
        )
        return False
    return True


def commit_table_lsn(argv, source_table, target_table, lsn):
    ssp_table = TableName(argv.auditschema,
                          const.SOURCE_SYSTEM_PROFILE_TABLE)
    sql = ("""
        UPDATE {ssp_table}
        SET min_lsn = %s, max_lsn = %s
        WHERE profile_name  = %s
          AND version       = %s
          AND source_region = %s
          AND target_region = %s
          AND object_name   = %s""".format(ssp_table=ssp_table))

    with get_audit_db(argv) as audit_db:
        if audit_db is None:
            return

        audit_db.execute(sql,
                         (lsn, lsn,
                          argv.profilename,
                          argv.profileversion,
                          source_table.schema,
                          target_table.schema,
                          source_table.name))
        audit_db.commit()


def extract(argv, fifo_file_path, table, column_list, source_db,
            source_conn_details, process_control_id, query_condition,
            extract_msg_queue):

    logging_loader.setup_logging(
        argv.workdirectory,
        "_".join([table.name, "extract"]))

    logger = logging.getLogger(__name__)

    mode = const.INITSYNCEXTRACT
    message = ("{worker} Starting {mode} for {table}"
               .format(mode=mode,
                       table=table.fullname,
                       worker=multiprocessing.current_process()
    ))
    logger.info(message)

    fifo = None
    record_count = 0
    result = {}
    lsn = None

    try:
        logger.debug("[{t}] Extract: Inserting process_control_detail record "
                     "for PC ID={id}"
                     .format(t=str(table), id=process_control_id))

        pc_detail = ProcessControlDetail(argv, mode, process_control_id)
        pc_detail.insert(
            comment=message,
            object_schema=table.schema,
            object_name=table.name,
            process_code=mode,
            infolog=logging_loader.get_logfile(const.INFO_HANDLER),
            errorlog=logging_loader.get_logfile(const.ERROR_HANDLER)
        )
        logger.debug("Successfully inserted process_control_detail record")

        try:
            fifo = build_file_writer(fifo_file_path)

            _wait_till_applier_is_ready(argv, logger)

            (message, lsn) = _extract_to_file(
                argv, fifo, column_list, table, query_condition,
                source_db, source_conn_details, logger, extract_msg_queue)

            logger.info(message)
            pc_detail.update(
                comment=message,
                status=const.SUCCESS,
                source_row_count=record_count,
                delta_startlsn=lsn,
                delta_endlsn=lsn,
                query_condition=query_condition
            )

            # Return the result back to the parent process via the shared queue
            result = (datetime.datetime.now(), lsn, const.SUCCESS, message)
            extract_msg_queue.put(result)
        except Exception, err:
            err_message = ("{worker} Failed extract: {err}"
                           .format(err=str(err),
                                   worker=multiprocessing.current_process())
            )
            report_error(argv, err_message, pc_detail, logger)
            result = (datetime.datetime.now(), lsn, const.ERROR, err_message)
            extract_msg_queue.put(result)

        logger.debug("{worker} Finished writing to fifo"
                     .format(worker=multiprocessing.current_process()))

    # A catchall to ensure a return message is sent
    except Exception, err:
        err_message = ("{worker} Failed extract: {err}"
                       .format(err=str(err),
                               worker=multiprocessing.current_process())
        )
        logger.exception(err_message)
        result = (datetime.datetime.now(), lsn, const.ERROR, err_message)
        extract_msg_queue.put(result)


def _wait_till_applier_is_ready(argv, logger):
    if argv.directload == const.GPLOAD:
        logger.debug("Allowing {t}s warmup time for applier"
                     .format(t=argv.extractwait))
        time.sleep(argv.extractwait)
        logger.debug("Done sleeping")

def _extract_to_file(argv, fifo, column_list, table, query_condition,
                     source_db, source_conn_details, logger,
                     extract_msg_queue):

    message = const.EMPTY_STRING

    lsn = None

    if argv.directunload == const.BCP:
        sql = source_db.build_extract_data_sql(
            column_list, table, argv.extractlsn,
            argv.samplerows, argv.lock, query_condition)

        cmd = (
            'bcp "{query}" '
            'queryout {filename} -S "{host},{port}" '
            '-U {user} -P {password} -d {database} -c -t{delim}').format(
                query=sql,
                filename=fifo.filename,
                host=source_conn_details.host,
                port=source_conn_details.port,
                user=source_conn_details.userid,
                password=source_conn_details.password,
                database=source_conn_details.dbsid,
                delim=argv.targetdelimiter,
            )

        logger.info("{worker} Executing external program: {cmd}"
                    .format(worker=multiprocessing.current_process(),
                            cmd=cmd))
        args = shlex.split(cmd)
        p = subprocess.Popen(args, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        (out, err) = p.communicate()
        errcode = p.returncode

        if errcode != 0:
            raise Exception("""
                Failed execution of: {cmd}
                ERRCODE: {errcode}
                STDOUT: {out}
                STDERR: {err}""".format(
                    cmd=cmd, errcode=errcode, out=out, err=err))

        result = row_count_re.search(out)
        if result:
            record_count = result.group(1)
        else:
            logger.warn("No record count found in bcp output: {}"
                        .format(out))
            record_count = 0
    else:
        logger.debug("source column_list = {l}".format(l=column_list))

        results = source_db.extract_data(
            column_list, table, query_condition, _log_extract_data_sql)

        raw_file = None
        if argv.rawfile is not None:
            filename = get_raw_filename(argv, table)
            raw_file = build_file_writer(filename)

        logger.debug("Writing to fifo")
        record_count = 0

        logger.debug("{worker} Fetching many ({n}) records at a time..."
                     .format(worker=multiprocessing.current_process(),
                             n=argv.arraysize))
        while True:
            records = results.fetchmany(argv.arraysize)
            for record in records:
                lsn = write(argv, record, fifo, raw_file)
                record_count += 1
                _log_progress(table, argv, record_count, logger)
                _send_heartbeat(record_count, extract_msg_queue)

            if len(records) < argv.arraysize:
                break

    return ("{worker} Extracted {count} {table} records"
            .format(worker=multiprocessing.current_process(),
                    count=record_count,
                    table=table.fullname),
            lsn)


def _log_extract_data_sql(argv, table, extract_data_sql):
    if argv.outputfile is not None:
        output_filename = get_output_filename(argv, table)
        output_file = build_file_writer(output_filename)

        output_file.write(extract_data_sql)
        output_file.close()


def _log_progress(table, argv, record_count, logger):
    if record_count % argv.auditcommitpoint == 0:
        logger.info("{worker} {table}: {count} records written to fifo..."
                    .format(worker=multiprocessing.current_process(),
                            table=table.name,
                            count=record_count))


def _send_heartbeat(record_count, extract_msg_queue):
    if record_count % const.HEARTBEAT_PERIOD == 0:
        msg = (datetime.datetime.now(), None, const.IN_PROGRESS, "Heartbeat")
        extract_msg_queue.put(msg)


def apply(argv, file_path, table, column_list, target_db,
          target_conn_details, process_control_id,
          query_condition, apply_msg_queue):

    logging_loader.setup_logging(
        argv.workdirectory,
        "_".join([table.name, "apply"]))
    logger = logging.getLogger(__name__)

    mode = const.INITSYNCAPPLY
    message = ("{worker} Starting {mode} for {table}"
               .format(worker=multiprocessing.current_process(),
                       mode=mode,
                       table=table.fullname)
    )

    logger.info(message)
    try:
        logger.debug("[{t}] Apply: Inserting process_control_detail record "
                     "for PC ID={id}"
                     .format(t=str(table), id=process_control_id))
        pc_detail = ProcessControlDetail(argv, mode, process_control_id)
        pc_detail.insert(
            object_schema=table.schema,
            object_name=table.name,
            comment=message,
            process_code=mode,
            infolog=logging_loader.get_logfile(const.INFO_HANDLER),
            errorlog=logging_loader.get_logfile(const.ERROR_HANDLER)
        )
        logger.debug("Successfully inserted process_control_detail record")

        if argv.inputfile:
            fifo = build_file_reader(argv.inputfile)
        else:
            fifo = build_file_reader(file_path)

        if argv.donotload:
            message = ("Do Not Load option set, apply aborted for {table}"
                       .format(table=table.fullname))
            logger.info(message)
            pc_detail.update(
                comment=message,
                status=const.SUCCESS
            )
        else:
            try:
                logger.debug("{worker} Getting source column list for applying"
                             .format(worker=multiprocessing.current_process()))

                # If we assume column definitions match between
                # source and target, then ensure we don't print the columns
                if argv.assumematchingcolumns:
                    column_list = None
                else:
                    def append_metacolname(argv, colname_key,
                                           colname_datatype, column_list):
                        colname = argv.metacols.get(colname_key)
                        if colname:
                            column_list.append({const.FIELD_NAME: colname, const.DATA_TYPE: colname_datatype, const.PARAMS: []})

                    map(lambda (colname_key, colname_datatype):
                        append_metacolname(
                            argv,
                            colname_key,
                            colname_datatype,
                            column_list
                        ),
                        [(const.METADATA_INSERT_TS_COL, "timestamp"),
                         (const.METADATA_UPDATE_TS_COL, "timestamp")]
                    )

                logger.info("Starting bulk write...")
                record_count = target_db.bulk_write(
                    input_file=fifo.handle,
                    table_name=table,
                    sep=argv.targetdelimiter,
                    null_string=argv.nullstring,
                    column_list=column_list,
                    quote_char=argv.quotechar,
                    escape_char=chr(const.ASCII_RECORDSEPARATOR),
                    size=argv.buffersize,
                    header=argv.header,
                    host=target_conn_details.host,
                    port=target_conn_details.port,
                    user=target_conn_details.userid,
                    password=target_conn_details.password,
                    database=target_conn_details.dbsid,
                    query_condition=query_condition,
                )

                target_db.commit()

                message = ("{worker} Applied {count} {table} records"
                           .format(worker=multiprocessing.current_process(),
                                   count=record_count,
                                   table=table.fullname))

                logger.info(message)
                pc_detail.update(
                    comment=message,
                    status=const.SUCCESS,
                    source_row_count=record_count,
                    insert_row_count=record_count,
                )
                result = (datetime.datetime.now(), const.SUCCESS, message)

                logger.debug("Writing apply result to queue: {}".format(result))
                apply_msg_queue.put(result)
            except Exception, err:
                err_message = ("{worker} Failed apply: {err}"
                               .format(err=str(err),
                                       worker=multiprocessing.current_process())
                )
                report_error(argv, err_message, pc_detail, logger)
                result = (datetime.datetime.now(), const.ERROR, err_message)
                apply_msg_queue.put(result)

    # A catchall to ensure a return message is sent
    except Exception, err:
        err_message = ("{worker} Failed apply: {err}"
                       .format(err=str(err),
                               worker=multiprocessing.current_process())
        )
        logger.exception(err_message)
        result = (datetime.datetime.now(), const.ERROR, err_message)
        apply_msg_queue.put(result)


def create_pipe(argv, table, logger):
    file_dir = argv.workdirectory
    file_path = os.path.join(file_dir, "{}.fifo".format(table.name))

    logger.info("Making fifo for writes: {file_path}".format(
        file_path=file_path))

    if not os.path.exists(file_path):
        mkdir_p(file_dir)
        os.mkfifo(file_path)
    elif not stat.S_ISFIFO(os.stat(file_path).st_mode):
        os.remove(file_path)
        os.mkfifo(file_path)

    logger.debug("Successfully made fifo")

    return file_path


def get_source_db(argv, conn_details, logger):
    return _connect_db(argv, argv.sourcedbtype, conn_details,
                       const.SOURCE, logger)


def get_target_db(argv, conn_details, logger):
    return _connect_db(argv, argv.targetdbtype, conn_details,
                       const.TARGET, logger)


def _connect_db(argv, dbtype, conn_details, sourceortarget, logger):
    logger.info("Connecting to {src_or_tgt} {dbtype} db"
                .format(src_or_tgt=sourceortarget, dbtype=dbtype))

    db = db_factory.build(dbtype, argv, logger, sourceortarget)
    db.connect(conn_details)

    logger.info("Connected to {src_or_tgt} {dbtype} db"
                .format(src_or_tgt=sourceortarget, dbtype=dbtype))
    return db


def report_error(argv, message, audit_object, logger, alert=False):
    try:
        logger.exception(message)
        if audit_object:
            audit_object.update(
                comment=message,
                status=const.ERROR
            )

        if alert:
            subject = ("{profile} InitSync ERROR"
                       .format(profile=argv.profilename))
            mailing_list = _build_mailing_list(argv, const.ERROR)

            mailer.send(argv.notifysender,
                        mailing_list,
                        subject,
                        argv.notifysmtpserver,
                        plain_text_message=message)

    except Exception, e:
        logger.exception("Failed to report error message: '{message}'. "
                         "Exception: {except_message}"
                         .format(message=message, except_message=str(e)))
        raise


def get_raw_filename(argv, table):
    return prefix_base_filename(table, argv.rawfile)


def get_output_filename(argv, table):
    return prefix_base_filename(table, argv.outputfile)


def prefix_base_filename(table, full_file_path):
    filename = os.path.basename(full_file_path)
    return full_file_path.replace(
        filename,
        "{table_name}_{filename}".format(table_name=table.name,
                                         filename=filename))


def value_to_str(v, encoding, nullstring):
    if v is None:
        return nullstring
    if isinstance(v, unicode):
        return v.encode(encoding)
    return str(v)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_source_system_profile_params(argv):
    """Gets source_system_profile parameters necessary for initsync"""
    with get_audit_db(argv) as audit_db:
        if audit_db is None:
            if not argv.tablelist:
                return []

            if len(argv.tablelist) == 1:
                # A file containing table names
                if os.path.isfile(argv.tablelist[0]):
                    with open(argv.tablelist[0]) as f:
                        return [(argv.sourceschema,
                                 t.strip(),
                                 argv.targetschema,
                                 None) for t in f]

            return [(argv.sourceschema, table, argv.targetschema, None)
                    for table in argv.tablelist]

        sql = """
        SELECT source_region, object_name, target_region, query_condition
        FROM {audit_schema}.source_system_profile
        WHERE profile_name = %s
          AND version      = %s
          AND active_ind   = 'Y'
        ORDER BY object_seq""".format(audit_schema=argv.auditschema)

        bind_values = [argv.profilename, argv.profileversion]
        result = audit_db.execute_query(sql, argv.arraysize, bind_values)

        return [(row[0], row[1], row[2], row[3]) for row in result]


def write(argv, record, fifo_file, output_file):
    lsn = None
    if argv.extractlsn:
        # pop off the last value which _should_ be the scn
        lsn = record[-1]
        record = list(record[:-1])
    else:
        record = list(record)

    record = [value_to_str(v, argv.clientencoding, argv.nullstring)
              for v in record]
    payload = "{csv}\n".format(csv=argv.targetdelimiter.join(record))

    fifo_file.write(payload)

    if output_file is not None:
        output_file.write(payload)

    return lsn


def report_no_active_schemas_tables(argv, process_control, logger):
    message = ("No active schemas/tables are configured "
               "for profile: {profile} with version: {version}"
               .format(profile=argv.profilename,
                       version=argv.profileversion))

    logger.debug(message)
    process_control.update(
        comment=message,
        status=const.WARNING
    )


def report_initsync_summary(argv, all_table_results, process_control,
                            starttime, endtime, last_message_offset):

    minlsn = const.CHAR_MAX_ASCII
    maxlsn = const.CHAR_MIN_ASCII
    summary_status = None
    erroneous_tables = []
    for table, result in all_table_results.items():
        (lsn, status, process_code, message) = result

        summary_status = combine_statuses(summary_status, status)
        if status is not None and status != const.SUCCESS:
            erroneous_tables.append((table, process_code, message))

        if lsn is not None:
            if lsn < minlsn:
                minlsn = lsn
            if lsn > maxlsn:
                maxlsn = lsn

    if minlsn == const.CHAR_MAX_ASCII:
        minlsn = None
    if maxlsn == const.CHAR_MIN_ASCII:
        maxlsn = None

    process_control.update(
        total_count=len(all_table_results),
        min_lsn=minlsn,
        # Ensures next CDC Extract commences from minlsn
        # to prevent data loss between minlsn and maxlsn
        max_lsn=minlsn,
        comment="Completed InitSync",
        status=summary_status,
        applier_marker=last_message_offset)

    _send_summary_email(argv, summary_status, all_table_results,
                        erroneous_tables, starttime, endtime)


def combine_statuses(prev_status, curr_status):
    if prev_status is None:
        return curr_status
    elif prev_status == const.SUCCESS and curr_status == const.ERROR:
        return const.WARNING
    elif prev_status == const.ERROR and curr_status == const.SUCCESS:
        return const.WARNING

    return prev_status


def _build_mailing_list(argv, status):
    mailing_list = set(argv.notifysummarylist)
    if status != const.SUCCESS:
        mailing_list = mailing_list.union(set(argv.notifyerrorlist))

    return mailing_list


def _send_summary_email(argv, status, all_table_results,
                        erroneous_tables, starttime, endtime):

    success_count = len(all_table_results) - len(erroneous_tables)
    success_rate = ("{success_count} out of {total}"
                    .format(success_count=success_count,
                            total=len(all_table_results)))
    subject_success_rate = const.EMPTY_STRING
    if status == const.WARNING:
        subject_success_rate = " ({sr})".format(sr=success_rate)

    mailing_list = _build_mailing_list(argv, status)

    subject = ("{profile} InitSync {status}{success_rate}".format(
               profile=argv.profilename,
               status=status,
               success_rate=subject_success_rate))

    duration = (endtime - starttime).total_seconds()

    # Sort by table name which is the first elem in the tuple
    erroneous_tables = sorted(erroneous_tables, key=lambda x: x[0])
    (plain_text_message, html_text_message) = _build_message_body(
        starttime, endtime, duration, erroneous_tables,
        success_rate, all_table_results)

    mailer.send(argv.notifysender,
                mailing_list,
                subject,
                argv.notifysmtpserver,
                plain_text_message=plain_text_message,
                html_text_message=html_text_message)


def _build_message_body(starttime, endtime, duration, erroneous_tables,
                        success_rate, all_table_results):

    plain_error_line = const.EMPTY_STRING
    if erroneous_tables:
        tables_with_delims = map(lambda t: " | ".join(t), erroneous_tables)
        tables_list = "\n".join(tables_with_delims)
        plain_error_line = ("Erroneous tables:\n{erroneous_tables}"
                            .format(erroneous_tables=tables_list))

    plain_text_message = """
{success_rate}

Start time  : {:%d %b %H:%M:%S}
End time    : {:%d %b %H:%M:%S}
Duration (s): {:.{prec}f}

{error_line}""".format(starttime, endtime, duration, prec=0,
                       success_rate=success_rate,
                       error_line=plain_error_line)

    html_error_line = const.EMPTY_STRING
    if erroneous_tables:
        tables_with_delims = map(lambda t: "</td><td>".join(t),
                                 erroneous_tables)
        tables_list = "</td></tr>\n<tr><td>".join(tables_with_delims)
        html_error_line = """
<h3>Erroneous tables</h3>
<table border="1" style="border-collapse:collapse;"
 cellpadding="5" cellspacing="0" summary="">
    <tr>
        <th>Tablename</th>
        <th>Process Code</th>
        <th>Error</th>
    </tr>
    <tr><td>{erroneous_tables}</td></tr>
</table>
""".format(erroneous_tables=tables_list)

    html_text_message = """
<h3>Summary</h3>
<table border="1" style="border-collapse:collapse;"
 cellpadding="5" cellspacing="0" summary="">
    <tr>
        <th>Start time</th>
        <th>End time</th>
        <th>Duration (s)</th>
        <th>Success Rate</th>
    </tr>
    <tr>
        <td align="center">{:%H:%M:%S %d/%m/%Y}</td>
        <td align="center">{:%H:%M:%S %d/%m/%Y}</td>
        <td align="center">{:.{prec}f}</td>
        <td align="center">{success_rate}</td>
    </tr>
</table>
<br>
<br>
{error_line}""".format(starttime, endtime, duration, prec=0,
                       success_rate=success_rate,
                       error_line=html_error_line)

    return (plain_text_message, html_text_message)


def _get_time_line(header, data, html_format=False):
    if html_format:
        starttime_string = """
        <tr>
            <th>{header}</th><td>{data}</td>
        </tr>""".format(header=header, data=data)
    else:
        starttime_string = "{header}: {data}".format(header=header, data=data)

    return starttime_string


def main():
    starttime = datetime.datetime.now()
    mode = const.INITSYNC
    argv = get_program_args(mode)

    logging_loader.setup_logging(argv.workdirectory, "main")
    logger = logging.getLogger(__name__)

    log_version(logger)

    process_control = ProcessControl(argv, mode)

    try:
        ssp_params = get_source_system_profile_params(argv)

        process_control.insert(
            comment="Started InitSync",
            source_system_code=argv.sourcesystem,
            infolog=logging_loader.get_logfile(const.INFO_HANDLER),
            errorlog=logging_loader.get_logfile(const.ERROR_HANDLER),
            total_count=0 if not ssp_params else len(ssp_params)
        )

        logger.debug("source_schemas_tables={}".format(ssp_params))

        if not ssp_params:
            report_no_active_schemas_tables(argv, process_control, logger)
            all_table_results = {}
        else:
            all_table_results = parallelise_initsync(
                argv, ssp_params,
                process_control.id,
                logger)

        endtime = datetime.datetime.now()

        # Set the offset marker to the end of queue
        kafka_consumer = build_kafka_consumer(
            argv,
            InitSyncKafkaClient(process_control, confluent_kafka.OFFSET_END))

        last_message_offset = -1
        if kafka_consumer:
            timeout = argv.consumertimeout
            last_message_offset = kafka_consumer.seek_to_end(timeout)

        report_initsync_summary(argv, all_table_results, process_control,
                                starttime, endtime, last_message_offset)
    except Exception, err:
        logger.exception(str(err))
        report_error(argv,
                     "Failed InitSync: {error}".format(error=str(err)),
                     process_control,
                     logger,
                     alert=True)


if __name__ == "__main__":
    main()
