###############################################################################
# Module:   apply
# Purpose:  Entry point for applier program
#
# Notes:
#
###############################################################################

import logging
import data_pipeline.constants.const as const
import data_pipeline.stream.factory as stream_factory
import data_pipeline.applier.factory as applier_factory
import data_pipeline.processor.factory as processor_factory
import data_pipeline.db.factory as db_factory
import data_pipeline.logger.logging_loader as logging_loader

from .common import set_process_control_schema, get_program_args, log_version
from data_pipeline.audit.factory import AuditFactory


def get_target_db(argv):
    return db_factory.build(argv.targetdbtype)


def build_applier(mode, argv):
    """
    Build an applier to apply CDCs to target db
    """
    source_processor = processor_factory.build(argv.sourcedbtype,
                                               argv.metacols)
    db = get_target_db(argv)
    return applier_factory.build(mode, source_processor,
                                 db, argv, AuditFactory(argv))


def main():
    mode = const.CDCAPPLY
    argv = get_program_args(mode)
    logging_loader.setup_logging(argv.workdirectory)
    logger = logging.getLogger(__name__)

    log_version(logger)

    set_process_control_schema(argv.auditschema)

    applier = build_applier(mode, argv)

    if argv.inputfile is not None:
        logger.info("Applying from file: {}".format(argv.inputfile))
        filereader = stream_factory.build_file_reader(argv.inputfile)
        filereader.read_to(applier)
    else:
        logger.info("Applying from kafka stream")
        kafka_consumer = stream_factory.build_kafka_consumer(argv, applier)

        if not kafka_consumer:
            logger.warn("Stream consumer is not defined! "
                        "Please check your configuration.")
        else:
            kafka_consumer.consumer_loop()


if __name__ == "__main__":
    main()
