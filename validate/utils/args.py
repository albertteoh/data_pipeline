##################################################################################
# Module:   args
# Purpose:  Module defining all switches and arguments used by verifier
#
# Notes:
#
##################################################################################

import sys
import logging
import data_pipeline.logger.logging_loader
import data_pipeline.constants.const as const
import argparse

logger = logging.getLogger(__name__)

def parse_args(arg_list):
    logger.info("Parsing command line arguments: {}".format(arg_list))

    args_parser = argparse.ArgumentParser()
    
    args_parser.add_argument("--audituser",       
        nargs='?', 
        required=True, 
        help="process audit user credentials requried for logging processing metrics")

    args_parser.add_argument("--validateuser",
        nargs='?', 
        required=True,
        help=("validate database user credentials in the form: dbuser/dbpasswd@SRCSID[:PORT]"))

    args_parser.add_argument(
        "--validatedbtype",
        nargs='?',
        required=True,
        choices=[const.ORACLE, const.MSSQL, const.POSTGRES],
        help="")

    args_parser.add_argument("--validatetag",
        nargs='?',
        required=True,
        help=("short friendly name for validation set as found in table validate.validate_tag"))



    args_parser.add_argument(
        "--arraysize",
        nargs='?',
        type=int,
        default=1000,
        help=("this read-write attribute specifies the number of rows to "
        "fetch at a time internally and is the default number of rows to "
        "fetch with the fetchmany() call. Note this attribute can drastically "
        "affect the performance of a query since it directly affects the "
        "number of network round trips that need to be performed."))
    
    args_parser.add_argument(
        "--workdirectory",
        nargs='?',
        required=True,
        help="output working directory")    


    parsed_args = args_parser.parse_args(arg_list)

    return parsed_args


def get_program_args():
    return parse_args(sys.argv[1:])

