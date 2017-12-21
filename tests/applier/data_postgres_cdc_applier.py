import data_pipeline.constants.const as const
from .data_common import TestCase, UPDATE_SSP_SQL


tests=[
  TestCase(
    description="Empty commit statement, no end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA],
    input_operation_codes=['', const.UPDATE],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0],
    input_commit_lsns=[0, 0],
    expect_sql_execute_called=[None, None],
    expect_execute_called_times=[0, 0],
    expect_audit_db_execute_sql_called=[None , None],
    expect_commit_called_times=[0, 0],
    expect_insert_row_count=[0, 0],
    expect_update_row_count=[0, 0],
    expect_delete_row_count=[0, 0],
    expect_source_row_count=[0, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED]
  )

,  TestCase(
    description="Empty commit statement, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        '',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, None, None],
    expect_execute_called_times=[0, 0, 0],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

,  TestCase(
    description="Single logminer redo statement, no end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')'],
    input_record_types=[const.START_OF_BATCH, const.DATA],
    input_operation_codes=['', const.INSERT],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0],
    input_commit_lsns=[0, 0],
    expect_sql_execute_called=[None, None],
    expect_execute_called_times=[0, 1],
    expect_audit_db_execute_sql_called=[None , None],
    expect_commit_called_times=[0, 0],
    expect_insert_row_count=[0, 1],
    expect_update_row_count=[0, 0],
    expect_delete_row_count=[0, 0],
    expect_source_row_count=[0, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED]
  )

, TestCase(
    description="Start of batch record only",
    input_table_name='',
    input_commit_statements=[''],
    input_record_types=[const.START_OF_BATCH],
    input_operation_codes=[''],
    input_primary_key_fields='',
    input_record_counts=[0],
    input_commit_lsns=[0],
    expect_sql_execute_called=[None],
    expect_execute_called_times=[0],
    expect_audit_db_execute_sql_called=[None],
    expect_commit_called_times=[0],
    expect_insert_row_count=[0],
    expect_update_row_count=[0],
    expect_delete_row_count=[0],
    expect_source_row_count=[0],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer insert redo statement containing column with special char, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","/COMPNDPK_2\\") values (\'26\',\'26.1\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( _COMPNDPK_2_, COMPNDPK_1 ) VALUES ( '26.1', '26' ); -- lsn: 0, offset: 1",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer insert redo statement containing primary key column with special char, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("/COMPNDPK_1\\","COMPNDPK_2") values (\'26\',\'26.1\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="/COMPNDPK_1\\",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( _COMPNDPK_1_, COMPNDPK_2 ) VALUES ( '26', '26.1' ); -- lsn: 0, offset: 1",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer update redo statement containing column with special char, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "/COL_V_2\\" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET _COL_V_2_ = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer update redo statement containing primary key column with special char, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = \'26.9\' where "/COMPNDPK_1\\" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="/COMPNDPK_1\\",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE _COMPNDPK_1_ = '26'; -- lsn: 0, offset: 1",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement containing parentheses in values with end of batch",
    input_table_name="IAG_MVDETAIL",
    input_commit_statements=[
        '',
        """insert into "KE_AGENTDESKTOP"."IAG_MVDETAIL"("COMPANY","SEQUENCE","MVYEAR","MVMAKE","MVMODEL","MVSERIES","MVBODY","MVENGTYP","MVENGCAP","MVEQUIP","STDEQUIP","MVTARE") values ('1','124049','2010','LOTUS','EVORA',NULL,'COUPE','FI','35','(2 SEAT)','ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS','0')""",
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPANY",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        """INSERT INTO ctl.IAG_MVDETAIL ( COMPANY, MVBODY, MVENGCAP, MVENGTYP, MVEQUIP, MVMAKE, MVMODEL, MVSERIES, MVTARE, MVYEAR, SEQUENCE, STDEQUIP ) VALUES ( '1', 'COUPE', '35', 'FI', '(2 SEAT)', 'LOTUS', 'EVORA', NULL, '0', '2010', '124049', 'ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS' ); -- lsn: 0, offset: 1""",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'iag_mvdetail'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement containing percent char in values with end of batch",
    input_table_name="IAG_MVDETAIL",
    input_commit_statements=[
        '',
        """insert into "KE_AGENTDESKTOP"."IAG_MVDETAIL"("COMPANY","SEQUENCE","MVYEAR","MVMAKE","MVMODEL","MVSERIES","MVBODY","MVENGTYP","MVENGCAP","MVEQUIP","STDEQUIP","MVTARE") values ('1','124049','2010','LOTUS','EVORA',NULL,'COUPE','FI','35','20%','ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS','0'); -- lsn: 0, offset: 1""",
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPANY",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        """INSERT INTO ctl.IAG_MVDETAIL ( COMPANY, MVBODY, MVENGCAP, MVENGTYP, MVEQUIP, MVMAKE, MVMODEL, MVSERIES, MVTARE, MVYEAR, SEQUENCE, STDEQUIP ) VALUES ( '1', 'COUPE', '35', 'FI', '20%%', 'LOTUS', 'EVORA', NULL, '0', '2010', '124049', 'ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS' ); -- lsn: 0, offset: 1""",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'iag_mvdetail'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer redo statement containing escaped double-single quotes",
    input_table_name="DE_SGI",
    input_commit_statements=[
        '',
        'update "KE_AGENTDESKTOP"."DE_SGI" set "ID" = \'68283124\', "CUSTOMERNAME_" = \'DENZIL\', "CUSTOMERQUERY_" = \' \', "CUSTOMEREMAIL_" = \'denzilhooya@yahoo.com\', "CUSTOMERPRODUCT_" = \'SSC-application\', "CUSTOMERGENDER_" = \'Male\', "CUSTOMERQUOTENUMBER_" = \'null\', "REFERRALID_" = \'null\', "SURNAME_" = \'D\\\'\'ROZARIO\', "BRAND_" = \'sgio\', "URLLOCATION_" = \'/sgio/ssc/pbtm-arrears/home/step1\', "USERNAME_" = \'denzilhooya@yahoo.com\', "CHATSERVEREMAIL_" = \'Not Specified\', "CUSTOMERKANAPARTYID_" = \'3255123\', "USERAGENT_" = \'Mozilla/5.0 (Linux; Android 6.0.1; SM-G900I Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36\' where "ID" = \'68283124\' and "CUSTOMERNAME_" = \'DENZIL\' and "CUSTOMERQUERY_" = \' \' and "CUSTOMEREMAIL_" = \'denzilhooya@yahoo.com\' and "CUSTOMERPRODUCT_" = \'SSC-application\' and "CUSTOMERGENDER_" = \'Male\' and "CUSTOMERQUOTENUMBER_" = \'null\' and "REFERRALID_" = \'null\' and "SURNAME_" = \'D\\\'\'ROZARIO\' and "BRAND_" = \'sgio\' and "URLLOCATION_" = \'/sgio/ssc/pbtm-arrears/home/step1\' and "USERNAME_" = \'denzilhooya@yahoo.com\' and "CHATSERVEREMAIL_" = \'Not Specified\' and "CUSTOMERKANAPARTYID_" = \'3255123\' and "USERAGENT_" = \'Mozilla/5.0 (Linux; Android 6.0.1; SM-G900I Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="ID",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "UPDATE ctl.DE_SGI SET CUSTOMERGENDER_ = 'Male', CUSTOMERPRODUCT_ = 'SSC-application', CUSTOMERNAME_ = 'DENZIL', CHATSERVEREMAIL_ = 'Not Specified', CUSTOMERKANAPARTYID_ = '3255123', USERNAME_ = 'denzilhooya@yahoo.com', CUSTOMERQUERY_ = ' ', CUSTOMEREMAIL_ = 'denzilhooya@yahoo.com', REFERRALID_ = 'null', CUSTOMERQUOTENUMBER_ = 'null', URLLOCATION_ = '/sgio/ssc/pbtm-arrears/home/step1', BRAND_ = 'sgio', ID = '68283124', SURNAME_ = 'D\\''ROZARIO', USERAGENT_ = 'Mozilla/5.0 (Linux; Android 6.0.1; SM-G900I Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36' WHERE ID = '68283124'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'de_sgi'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply logminer insert statement with null with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),NULL,\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', NULL, '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        None],
                                      
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 1, 1],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )


, TestCase(
    description="Apply insert and update statements each with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        '',
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, '', '', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1, 0, 0, 1],
    input_commit_lsns=[0, 0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        None,
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1, 1, 1, 2],
    expect_insert_row_count=[0, 1, 1, 1, 1, 1],
    expect_update_row_count=[0, 0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 1, 1, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

  )

, TestCase(
    description="Apply insert and update statements with nulls each with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),NULL,\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        '',
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = NULL where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, '', '', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1, 0, 0, 1],
    input_commit_lsns=[0, 0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', NULL, '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        None,
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = NULL WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1, 1, 1, 2],
    expect_insert_row_count=[0, 1, 1, 1, 1, 1],
    expect_update_row_count=[0, 0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 1, 1, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply insert and update statements with single quotes each with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        """insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COL_NAS_9") values ('26','string ''with quotes''')'""",
        '',
        '',
        """update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = 'string ''with quotes''' where "COMPNDPK_1" = \'26\'""",
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH, const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, '', '', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1, 0, 0, 1],
    input_commit_lsns=[0, 0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COMPNDPK_1 ) VALUES ( 'string ''with quotes''', '26' ); -- lsn: 0, offset: 1",
        None,
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = 'string ''with quotes''' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1"],
    expect_execute_called_times=[0, 1, 1, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1, 1, 1, 2],
    expect_insert_row_count=[0, 1, 1, 1, 1, 1],
    expect_update_row_count=[0, 0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0, 0],
    expect_source_row_count=[0, 1, 1, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]

  )


, TestCase(
    description="Apply update statement with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply update statement with SET on PK, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COMPNDPK_1" = \'27\', "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9', COMPNDPK_1 = '27' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply consecutive insert statements with single end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'27\',\'27.1\',\'27.2\',\'27.3\',\'27.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'27_varchar_1\',\'27_varchar_2\',\'27_varchar_3\',\'27_varchar_4\',\'27.5\',\'27.6\',\'27.7\',\'27.8\',\'27.9\',\'This is a nasty string ??a??????????\')',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, const.INSERT, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 0, 2],
    input_commit_lsns=[0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '27.5', '27.6', '27.7', '27.8', '27.9', '2017-04-19 12:14:22', '27_varchar_1', '27_varchar_2', '27_varchar_3', '27_varchar_4', '27', '27.1', '27.2', '27.3', '27.4' ); -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 0, 1],
    expect_insert_row_count=[0, 1, 2, 2],
    expect_update_row_count=[0, 0, 0, 0],
    expect_delete_row_count=[0, 0, 0, 0],
    expect_source_row_count=[0, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )


, TestCase(
    description="Apply consecutive insert statements, followed by update with single end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'27\',\'27.1\',\'27.2\',\'27.3\',\'27.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'27_varchar_1\',\'27_varchar_2\',\'27_varchar_3\',\'27_varchar_4\',\'27.5\',\'27.6\',\'27.7\',\'27.8\',\'27.9\',\'This is a nasty string ??a??????????\')',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.DATA, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, const.INSERT, const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 0, 0, 3],
    input_commit_lsns=[0, 0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '27.5', '27.6', '27.7', '27.8', '27.9', '2017-04-19 12:14:22', '27_varchar_1', '27_varchar_2', '27_varchar_3', '27_varchar_4', '27', '27.1', '27.2', '27.3', '27.4' ); -- lsn: 0, offset: 1",
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 2, 3, 3],
    expect_audit_db_execute_sql_called=[None, None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 0, 0, 1],
    expect_insert_row_count=[0, 1, 2, 2, 2],
    expect_update_row_count=[0, 0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0, 0],
    expect_source_row_count=[0, 1, 2, 3, 3],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )



, TestCase(
    description="Apply consecutive insert and update statements with single end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),\'26_varchar_1\',\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 0, 2],
    input_commit_lsns=[0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', '26_varchar_1', '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = '26.9' WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 0, 1],
    expect_insert_row_count=[0, 1, 1, 1],
    expect_update_row_count=[0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0],
    expect_source_row_count=[0, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply consecutive insert and update statements with nulls with single end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'insert into "SYS"."CONNCT_CDC_PK5_COLS10"("COMPNDPK_1","COMPNDPK_2","COMPNDPK_3","COMPNDPK_4","COMPNDPK_5","COL_TS_0","COL_V_1","COL_V_2","COL_V_3","COL_V_4","COL_N_5","COL_N_6","COL_N_7","COL_N_8","COL_N_9","COL_NAS_9") values (\'26\',\'26.1\',\'26.2\',\'26.3\',\'26.4\',TO_TIMESTAMP(\'2017-04-19 12:14:22\'),NULL,\'26_varchar_2\',\'26_varchar_3\',\'26_varchar_4\',\'26.5\',\'26.6\',\'26.7\',\'26.8\',\'26.9\',\'This is a nasty string ??a??????????\')',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COL_V_2" = NULL where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.INSERT, const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 0, 2],
    input_commit_lsns=[0, 0, 0, 0],
    expect_sql_execute_called=[
        None,
        "INSERT INTO ctl.CONNCT_CDC_PK5_COLS10 ( COL_NAS_9, COL_N_5, COL_N_6, COL_N_7, COL_N_8, COL_N_9, COL_TS_0, COL_V_1, COL_V_2, COL_V_3, COL_V_4, COMPNDPK_1, COMPNDPK_2, COMPNDPK_3, COMPNDPK_4, COMPNDPK_5 ) VALUES ( 'This is a nasty string ??a??????????', '26.5', '26.6', '26.7', '26.8', '26.9', '2017-04-19 12:14:22', NULL, '26_varchar_2', '26_varchar_3', '26_varchar_4', '26', '26.1', '26.2', '26.3', '26.4' ); -- lsn: 0, offset: 1",
        "UPDATE ctl.CONNCT_CDC_PK5_COLS10 SET COL_V_2 = NULL WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1",
        None],
    expect_execute_called_times=[0, 1, 2, 2],
    expect_audit_db_execute_sql_called=[None, None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 0, 1],
    expect_insert_row_count=[0, 1, 1, 1],
    expect_update_row_count=[0, 0, 1, 1],
    expect_delete_row_count=[0, 0, 0, 0],
    expect_source_row_count=[0, 1, 2, 2],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply delete statement with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'delete from "SYS"."CONNCT_CDC_PK5_COLS10" where "COMPNDPK_1" = \'26\'', None],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DELETE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "DELETE FROM ctl.CONNCT_CDC_PK5_COLS10 WHERE COMPNDPK_1 = '26'; -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 1, 1],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply delete statement containing special char in column name, with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'delete from "SYS"."CONNCT_CDC_PK5_COLS10" where "/COMPNDPK_1\\" = \'26\'', None],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DELETE, ''],
    input_primary_key_fields="/COMPNDPK_1\\",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "DELETE FROM ctl.CONNCT_CDC_PK5_COLS10 WHERE _COMPNDPK_1_ = '26'; -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 1, 1],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply create statement with character data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'create table "SYS"."CONNCT_CDC_PK5_COLS10" (COMPNDPK_1 varchar(5), COMPNDPK_2 varchar2(5), COMPNDPK_3 nvarchar2(5), COMPNDPK_4 char(5), COMPNDPK_5 nchar(5))', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "CREATE TABLE ctl.CONNCT_CDC_PK5_COLS10 (COMPNDPK_1 VARCHAR(5), COMPNDPK_2 VARCHAR(5), COMPNDPK_3 VARCHAR(5), COMPNDPK_4 CHAR(5), COMPNDPK_5 CHAR(5), ctl_upd_ts TIMESTAMP, ctl_ins_ts TIMESTAMP); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply create statement with number data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'create table "SYS"."CONNCT_CDC_PK5_COLS10" (SMALLINT0 number(1), SMALLINT1 number(1, 0), SMALLINT2 number(1, 1), SMALLINT3 number(1, -1), INT0 number(5), INT1 number(5, 0), INT2 number(5, 2), INT3 number(5, -2), BIGINT0 number(10), BIGINT1 number(10, 0), BIGINT2 number(10, 2), BIGINT3 number(10, -2), NUMERIC0 number(19), NUMERIC1 number(19, 0), NUMERIC2 number(19, -1) )', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="SMALLINT0",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "CREATE TABLE ctl.CONNCT_CDC_PK5_COLS10 (SMALLINT0 SMALLINT, SMALLINT1 SMALLINT, SMALLINT2 NUMERIC, SMALLINT3 NUMERIC, INT0 INTEGER, INT1 INTEGER, INT2 NUMERIC, INT3 NUMERIC, BIGINT0 BIGINT, BIGINT1 BIGINT, BIGINT2 NUMERIC, BIGINT3 NUMERIC, NUMERIC0 NUMERIC, NUMERIC1 NUMERIC, NUMERIC2 NUMERIC, ctl_upd_ts TIMESTAMP, ctl_ins_ts TIMESTAMP); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply create statement with date data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'create table "SYS"."CONNCT_CDC_PK5_COLS10" (COMPNDPK_1 date)', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "CREATE TABLE ctl.CONNCT_CDC_PK5_COLS10 (COMPNDPK_1 DATE, ctl_upd_ts TIMESTAMP, ctl_ins_ts TIMESTAMP); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply create statement with time data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'create table "SYS"."CONNCT_CDC_PK5_COLS10" (COMPNDPK_1 time)', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "CREATE TABLE ctl.CONNCT_CDC_PK5_COLS10 (COMPNDPK_1 TIME, ctl_upd_ts TIMESTAMP, ctl_ins_ts TIMESTAMP); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply create statement with timestamp data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'create table "SYS"."CONNCT_CDC_PK5_COLS10" (COMPNDPK_1 timestamp, COMPNDPK_2 timestamp with time zone)', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "CREATE TABLE ctl.CONNCT_CDC_PK5_COLS10 (COMPNDPK_1 TIMESTAMP, COMPNDPK_2 TIMESTAMP with time zone, ctl_upd_ts TIMESTAMP, ctl_ins_ts TIMESTAMP); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply real-world create statement",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', """                 CREATE TABLE sys.CONNCT_CDC_PK5_COLS10  ( compndPk_1 NUMBER(15,5) NOT NULL, compndPk_2 NUMBER(15,5) NOT NULL, compndPk_3 NUMBER(15,5) NOT NULL, compndPk_4 NUMBER(15,5) NOT NULL, compndPk_5 NUMBER(15,5) NOT NULL, col_ts_0      TIMESTAMP(6), col_v_1 \tVARCHAR2(25) NULL, col_v_2 \tVARCHAR2(25) NULL, col_v_3 \tVARCHAR2(25) NULL, col_v_4 \tVARCHAR2(25) NULL, col_n_5 \tNUMBER(15,5) NULL, col_n_6 \tNUMBER(15,5) NULL, col_n_7 \tNUMBER(15,5) NULL, col_n_8 \tNUMBER(15,5) NULL, col_n_9 \tNUMBER(15,5) NULL, col_nas_9 \tVARCHAR2(4000) NULL,CONSTRAINT cnst_CONNCT_CDC_PK5_COLS10 PRIMARY KEY( compndPk_1, compndPk_2, compndPk_3, compndPk_4, compndPk_5))""", ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, """CREATE TABLE ctl.CONNCT_CDC_PK5_COLS10 (compndPk_1 NUMERIC NOT NULL, compndPk_2 NUMERIC NOT NULL, compndPk_3 NUMERIC NOT NULL, compndPk_4 NUMERIC NOT NULL, compndPk_5 NUMERIC NOT NULL, col_ts_0 TIMESTAMP, col_v_1 VARCHAR(25) NULL, col_v_2 VARCHAR(25) NULL, col_v_3 VARCHAR(25) NULL, col_v_4 VARCHAR(25) NULL, col_n_5 NUMERIC NULL, col_n_6 NUMERIC NULL, col_n_7 NUMERIC NULL, col_n_8 NUMERIC NULL, col_n_9 NUMERIC NULL, col_nas_9 VARCHAR(4000) NULL, CONSTRAINT cnst_CONNCT_CDC_PK5_COLS10 PRIMARY KEY( compndPk_1, compndPk_2, compndPk_3, compndPk_4, compndPk_5), ctl_upd_ts TIMESTAMP, ctl_ins_ts TIMESTAMP); -- lsn: 0, offset: 1""", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply alter supplemental statement should skip",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'ALTER TABLE CONNCT_CDC_PK5_COLS10 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, None, None],
    expect_execute_called_times=[0, 0, 0],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )


, TestCase(
    description="Apply alter statement with character data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'alter table "SYS"."CONNCT_CDC_PK5_COLS10" add COMPNDPK_1 varchar(5), add COMPNDPK_2 varchar2(5), add COMPNDPK_3 nvarchar2(5), add COMPNDPK_4 char(5), modify COMPNDPK_5 nchar(5)', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "ALTER TABLE ctl.CONNCT_CDC_PK5_COLS10 ADD COMPNDPK_1 VARCHAR(5), ADD COMPNDPK_2 VARCHAR(5), ADD COMPNDPK_3 VARCHAR(5), ADD COMPNDPK_4 CHAR(5), ALTER COMPNDPK_5 TYPE CHAR(5); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply alter statement with number data types",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=['', 'alter table "SYS"."CONNCT_CDC_PK5_COLS10" add SMALLINT0 number(1), add SMALLINT1 number(1, 0), add SMALLINT2 number(1, 1), add SMALLINT3 number(1, -1), add INT0 number(5), add INT1 number(5, 0), add INT2 number(5, 2), add INT3 number(5, -2), add BIGINT0 number(10), add BIGINT1 number(10, 0), add BIGINT2 number(10, 2), add BIGINT3 number(10, -2), add NUMERIC0 number(19), add NUMERIC1 number(19, 0), modify NUMERIC2 number(19, -1)', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields="SMALLINT0",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "ALTER TABLE ctl.CONNCT_CDC_PK5_COLS10 ADD SMALLINT0 SMALLINT, ADD SMALLINT1 SMALLINT, ADD SMALLINT2 NUMERIC, ADD SMALLINT3 NUMERIC, ADD INT0 INTEGER, ADD INT1 INTEGER, ADD INT2 NUMERIC, ADD INT3 NUMERIC, ADD BIGINT0 BIGINT, ADD BIGINT1 BIGINT, ADD BIGINT2 NUMERIC, ADD BIGINT3 NUMERIC, ADD NUMERIC0 NUMERIC, ADD NUMERIC1 NUMERIC, ALTER NUMERIC2 TYPE NUMERIC; -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply alter MODIFY statement",
    input_table_name="CWQ_QUEUED_WORK_TYPE",
    input_commit_statements=['', 'ALTER TABLE CWQ_QUEUED_WORK_TYPE MODIFY DESCRIPTION VARCHAR2(400 CHAR) ', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields=None,
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "ALTER TABLE ctl.CWQ_QUEUED_WORK_TYPE ALTER DESCRIPTION TYPE VARCHAR(400); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'cwq_queued_work_type'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Apply alter ADD statement",
    input_table_name="CWQ_QUEUED_WORK_TYPE",
    input_commit_statements=['', 'ALTER TABLE CWQ_QUEUED_WORK_TYPE ADD DESCRIPTION VARCHAR2(400 CHAR) ', ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.DDL, ''],
    input_primary_key_fields=None,
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[None, "ALTER TABLE ctl.CWQ_QUEUED_WORK_TYPE ADD DESCRIPTION VARCHAR(400); -- lsn: 0, offset: 1", None],
    expect_execute_called_times=[0, 1, 1],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'cwq_queued_work_type'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 0, 0],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED]
  )

, TestCase(
    description="Kill record only",
    input_table_name='',
    input_commit_statements=[''],
    input_record_types=[const.KILL],
    input_operation_codes=[''],
    input_primary_key_fields='',
    input_record_counts=[0],
    input_commit_lsns=[0],
    expect_sql_execute_called=[None],
    expect_execute_called_times=[0],
    expect_audit_db_execute_sql_called=[None],
    expect_commit_called_times=[0],
    expect_insert_row_count=[0],
    expect_update_row_count=[0],
    expect_delete_row_count=[0],
    expect_source_row_count=[0],
    expect_batch_committed=[const.KILLED]
  )

, TestCase(
    description="Start then Kill",
    input_table_name='',
    input_commit_statements=['', ''],
    input_record_types=[const.START_OF_BATCH, const.KILL],
    input_operation_codes=['', ''],
    input_primary_key_fields='',
    input_record_counts=[0, 0],
    input_commit_lsns=[0, 0],
    expect_sql_execute_called=[None, None],
    expect_execute_called_times=[0, 0],
    expect_audit_db_execute_sql_called=[None , None],
    expect_commit_called_times=[0, 0],
    expect_insert_row_count=[0, 0],
    expect_update_row_count=[0, 0],
    expect_delete_row_count=[0, 0],
    expect_source_row_count=[0, 0],
    expect_batch_committed=[const.UNCOMMITTED, const.KILLED]
  )

]
