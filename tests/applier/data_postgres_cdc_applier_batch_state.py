# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
import collections
import data_pipeline.constants.const as const

TARGETSCHEMA = 'ctl'

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statements input_record_types input_operation_codes input_primary_key_fields ")

tests=[
  TestCase(
    description="End of batch without start",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=[''],
    input_record_types=[const.END_OF_BATCH],
    input_operation_codes=[''],
    input_primary_key_fields="COMPNDPK_1",
  )

, TestCase(
    description="End of batch without start, followed by data",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=[
        '', 
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')'], 
    input_record_types=[const.END_OF_BATCH, const.DATA],
    input_operation_codes=['', const.INSERT],
    input_primary_key_fields="COMPNDPK_1",
  )

, TestCase(
    description="Start of batch after already started",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=['', ''],
    input_record_types=[const.START_OF_BATCH, const.START_OF_BATCH],
    input_operation_codes=['', ''],
    input_primary_key_fields="COMPNDPK_1",
  )

, TestCase(
    description="Second end of batch after valid batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_commit_statements=['', 
        'update CONNCT_CDC_PK5_COLS10 set "blah" = \'1\' where "COMPNDPK_1" = \'2\'', 
        '', 
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, '', ''],
    input_primary_key_fields="COMPNDPK_1",
  )

]
