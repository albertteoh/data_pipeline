from sqlalchemy import create_engine
from sqlalchemy import Boolean, Column, String, Integer, DateTime, Numeric, Text, Sequence, BigInteger, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#db_string = "postgres://testuser:pa55word@localhost:5432/testdb"
#db_string ="postgres://test:test1234@13.54.63.57:5432/iag"
#db_string ="postgres://test:test1234@13.54.17.118:5433/iag"
db_string = "postgres://sys_datapipeline:datapipeline_P0stgr35@10.137.206.165:6432/edhpreprod"

db = create_engine(db_string)
Base = declarative_base()

class ProcessControl(Base):
    __tablename__ = 'process_control'

    id = Column(Integer, Sequence('process_control_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    profile_version = Column(Integer)
    process_code = Column(String(20))
    process_name = Column(String(30))
    source_system_code = Column(String(30))
    source_system_type = Column(String(10))
    source_region = Column(String(256))
    target_system = Column(String(30))
    target_system_type = Column(String(10))
    target_region = Column(String(256))
    process_starttime = Column(DateTime)
    process_endtime = Column(DateTime)
    min_lsn = Column(String(30))
    max_lsn = Column(String(30))
    status = Column(String(20))
    duration = Column(Numeric(precision=20, scale=4))
    dml_count = Column(BigInteger)
    ddl_count = Column(BigInteger)
    other_count = Column(BigInteger)
    total_count = Column(BigInteger)               
    comment = Column(String(4000))
    filename = Column(String(1024))
    infolog = Column(String(1024))
    errorlog = Column(String(1024))
    applier_marker = Column(BigInteger)
    executor_status = Column(String(10))
    executor_logs = Column(Integer)
    archive_logs = Column(Integer)          
    object_list = Column(Text)
      
class ProcessControlDetail(Base):
    __tablename__ = 'process_control_detail'

    id = Column(Integer, Sequence('process_control_detail_id_seq'), primary_key=True)
    run_id = Column(Integer, ForeignKey('process_control.id'))
    process_code = Column(String(20))
    object_schema = Column(String(30))
    object_name = Column(String(50))
    process_starttime = Column(DateTime)
    process_endtime = Column(DateTime)
    status = Column(String(20))
    source_row_count = Column(BigInteger)
    insert_row_count = Column(BigInteger)
    update_row_count = Column(BigInteger)
    delete_row_count = Column(BigInteger)
    bad_row_count = Column(BigInteger)
    alter_count = Column(BigInteger)
    create_count = Column(BigInteger)
    delta_starttime = Column(DateTime)
    delta_endtime = Column(DateTime)
    delta_startlsn = Column(String(30))
    delta_endlsn = Column(String(30))
    error_message = Column(String(300))
    comment = Column(String(300))
    filename = Column(String(4000))
    linked_run_id = Column(BigInteger)
    query_condition = Column(String(4000))
    errorlog = Column(String(1024))
    infolog = Column(String(1024))
    duration = Column(Numeric(precision=20, scale=4))
           
class SourceSystemProfile(Base):
    __tablename__ = 'source_system_profile'
 
    id = Column(Integer, Sequence('source_system_profile_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    version = Column(Integer)
    source_system_code = Column(String(30))
    source_region = Column(String(256))
    target_region = Column(String(256))
    object_name = Column(String(50))
    object_seq = Column(BigInteger)
    min_lsn = Column(String(30))
    max_lsn = Column(String(30))
    active_ind = Column(String(1))
    history_ind = Column(String(1))
    applied_ind = Column(String(1))
    delta_ind = Column(String(1))
    last_run_id = Column(Integer)
    last_process_code = Column(String(20))
    last_status = Column(String(20))
    last_updated = Column(DateTime)
    last_applied = Column(DateTime)
    last_history_update = Column(DateTime)
    notes = Column(String(4000))
    query_condition = Column(String(4000))

class Connections(Base):
    __tablename__ = 'connections'
 
    id = Column(Integer, Sequence('connections_id_seq'), primary_key=True)
    connection_name = Column(String(30))
    connection_category = Column(String(10))
    database_type = Column(String(10))
    hostname = Column(String(100))
    portnumber = Column(Integer)
    username = Column(String(50))
    password = Column(String(50))
    database_name = Column(String(50))    
    created_by = Column(String(50))
    created_date = Column(DateTime)
    updated_by = Column(String(50))
    updated_date = Column(DateTime)
    notes = Column(String(200))

class ProcessParameters(Base):
    __tablename__ = 'process_parameters'
 
    id = Column(Integer, Sequence('process_parameters_id_seq'), primary_key=True)
    parameter_name = Column(String(80))
    parameter_type = Column(String(10))
    parameter_value = Column(String(300))
 
class ReferenceData(Base):
    __tablename__ = 'reference_data'
 
    id = Column(Integer, Sequence('reference_data_id_seq'), primary_key=True)
    domain = Column(String(50))
    code = Column(String(30))
    description = Column(String(1000))
    active_ind = Column(String(1))
    order_seq = Column(Integer)
     
class User(Base):
    __tablename__ = "users"
    id = Column('user_id',Integer , primary_key=True)
    username = Column('username', String(20), unique=True , index=True)
    firstname = Column('firstname', String(20))
    lastname = Column('lastname', String(20))
    password = Column('password' , String(10))
    email = Column('email',String(50),unique=True , index=True)
    role = Column('role' , String(10))
    registered_on = Column('registered_on' , DateTime)

class Profile(Base):
    __tablename__ = 'profile'
 
    id = Column(Integer, Sequence('profile_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    version = Column(Integer)
    source_system = Column(String(30))
    source_system_code = Column(String(30))
    source_database_type = Column(String(30))
    source_connection = Column(String(30))
    target_system = Column(String(30))
    target_system_code = Column(String(30))
    target_database_type = Column(String(30))
    target_connection = Column(String(30))
    description = Column(String(1000))
    active_ind = Column(String(1))    
    server_path = Column(String(4000))
    
Session = sessionmaker(db)
session = Session()

Base.metadata.create_all(db)



