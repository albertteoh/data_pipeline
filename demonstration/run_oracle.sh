#!/bin/bash

export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
export ORACLE_SID=XE
export PATH=${PATH}:${ORACLE_HOME}/bin

sqlplus ${1} ${2} ${3}
