import data_pipeline.constants.const as const
import data_postgres_cdc_applier 
from .data_common import TestCase, UPDATE_SSP_SQL


tests = [

 TestCase(
    description="Apply update statement containing a single primary key SET with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COMPNDPK_1" = \'0\' where "COMPNDPK_1" = \'26\'',
        ''],
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_operation_codes=['', const.UPDATE, ''],
    input_primary_key_fields="COMPNDPK_1",
    input_record_counts=[0, 0, 1],
    input_commit_lsns=[0, 0, 0],
    expect_sql_execute_called=[
        None,
        None,
        None],
    expect_execute_called_times=[0, 0, 0],
    expect_audit_db_execute_sql_called=[None, None, (UPDATE_SSP_SQL, ('CDCApply', 0, 'myprofile', 1, 'ctl', 'connct_cdc_pk5_cols10'))],
    expect_commit_called_times=[0, 0, 1],
    expect_insert_row_count=[0, 0, 0],
    expect_update_row_count=[0, 1, 1],
    expect_delete_row_count=[0, 0, 0],
    expect_source_row_count=[0, 1, 1],
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

  )

, TestCase(
    description="Apply update statement containing a primary key and non-primary key in SET with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10",
    input_commit_statements=[
        '',
        'update "SYS"."CONNCT_CDC_PK5_COLS10" set "COMPNDPK_1" = \'0\', "COL_V_2" = \'26.9\' where "COMPNDPK_1" = \'26\'',
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
    expect_batch_committed=[const.UNCOMMITTED, const.UNCOMMITTED, const.COMMITTED,]

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
        "UPDATE ctl.DE_SGI SET CUSTOMERGENDER_ = 'Male', CUSTOMERPRODUCT_ = 'SSC-application', CUSTOMERNAME_ = 'DENZIL', CHATSERVEREMAIL_ = 'Not Specified', CUSTOMERKANAPARTYID_ = '3255123', USERNAME_ = 'denzilhooya@yahoo.com', CUSTOMERQUERY_ = ' ', CUSTOMEREMAIL_ = 'denzilhooya@yahoo.com', REFERRALID_ = 'null', CUSTOMERQUOTENUMBER_ = 'null', URLLOCATION_ = '/sgio/ssc/pbtm-arrears/home/step1', BRAND_ = 'sgio', SURNAME_ = 'D\\''ROZARIO', USERAGENT_ = 'Mozilla/5.0 (Linux; Android 6.0.1; SM-G900I Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36' WHERE ID = '68283124'; -- lsn: 0, offset: 1",
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


]
