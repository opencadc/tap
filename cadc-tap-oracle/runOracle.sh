#!/bin/bash

############# Execute custom scripts ##############
function runUserScripts {

  SCRIPTS_ROOT="$1";

  # Check whether parameter has been passed on
  if [ -z "$SCRIPTS_ROOT" ]; then
    echo "$0: No SCRIPTS_ROOT passed on, no scripts will be run";
    exit 1;
  fi;
  
  # Execute custom provided files (only if directory exists and has files in it)
  if [ -d "$SCRIPTS_ROOT" ] && [ -n "$(ls -A $SCRIPTS_ROOT)" ]; then
      
    echo "";
    echo "Executing user defined scripts"
  
    for f in $SCRIPTS_ROOT/*; do
        case "$f" in
            *.sh)     echo "$0: running $f"; . "$f" ;;
            *.sql)    echo "$0: running $f"; echo "exit" | su -p oracle -c "$ORACLE_HOME/bin/sqlplus / as sysdba @$f"; echo ;;
            *)        echo "$0: ignoring $f" ;;
        esac
        echo "";
    done
    
    echo "DONE: Executing user defined scripts"
    echo "";
  
  fi;
  
}

########### Move DB files ############
function moveFiles {
   if [ ! -d $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID ]; then
      su -p oracle -c "mkdir -p $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/"
   fi;
   
   su -p oracle -c "mv $ORACLE_HOME/dbs/spfile$ORACLE_SID.ora $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/"
   su -p oracle -c "mv $ORACLE_HOME/dbs/orapw$ORACLE_SID $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/"
   su -p oracle -c "mv $ORACLE_HOME/network/admin/listener.ora $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/"
   su -p oracle -c "mv $ORACLE_HOME/network/admin/tnsnames.ora $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/"
   mv /etc/sysconfig/oracle-xe $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/
      
   symLinkFiles;
}

########### Symbolic link DB files ############
function symLinkFiles {

   if [ ! -L $ORACLE_HOME/dbs/spfile$ORACLE_SID.ora ]; then
      ln -s $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/spfile$ORACLE_SID.ora $ORACLE_HOME/dbs/spfile$ORACLE_SID.ora
   fi;
   
   if [ ! -L $ORACLE_HOME/dbs/orapw$ORACLE_SID ]; then
      ln -s $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/orapw$ORACLE_SID $ORACLE_HOME/dbs/orapw$ORACLE_SID
   fi;
   
   if [ ! -L $ORACLE_HOME/network/admin/listener.ora ]; then
      ln -sf $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/listener.ora $ORACLE_HOME/network/admin/listener.ora
   fi;
   
   if [ ! -L $ORACLE_HOME/network/admin/tnsnames.ora ]; then
      ln -sf $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/tnsnames.ora $ORACLE_HOME/network/admin/tnsnames.ora
   fi;
   
   if [ ! -L /etc/sysconfig/oracle-xe ]; then
      ln -s $ORACLE_BASE/oradata/dbconfig/$ORACLE_SID/oracle-xe /etc/sysconfig/oracle-xe
   fi;
}

########### SIGTERM handler ############
function _term() {
   echo "Stopping container."
   echo "SIGTERM received, shutting down database!"
  /etc/init.d/oracle-xe stop
}

########### SIGKILL handler ############
function _kill() {
   echo "SIGKILL received, shutting down database!"
   /etc/init.d/oracle-xe stop
}

############# Create DB ################
function createDB {
   # Auto generate ORACLE PWD if not passed on
   export ORACLE_PWD=${ORACLE_PWD:-"`openssl rand -hex 8`"}
   echo "ORACLE PASSWORD FOR SYS AND SYSTEM: $ORACLE_PWD";

   sed -i -e "s|###ORACLE_PWD###|$ORACLE_PWD|g" $ORACLE_BASE/$CONFIG_RSP && \
   /etc/init.d/oracle-xe configure responseFile=$ORACLE_BASE/$CONFIG_RSP

   echo "Database configured.\n ${ORACLE_BASE}/${CONFIG_RSP}"

   # Listener 
   echo "# listener.ora Network Configuration File:
         
         SID_LIST_LISTENER = 
           (SID_LIST =
             (SID_DESC =
               (SID_NAME = PLSExtProc)
               (ORACLE_HOME = $ORACLE_HOME)
               (PROGRAM = extproc)
             )
           )
         
         LISTENER =
           (DESCRIPTION_LIST =
             (DESCRIPTION =
               (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC_FOR_XE))
               (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))
             )
           )
         
         DEFAULT_SERVICE_LISTENER = (XE)" > $ORACLE_HOME/network/admin/listener.ora

# TNS Names.ora
   echo "# tnsnames.ora Network Configuration File:

XE =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = XE)
    )
  )

EXTPROC_CONNECTION_DATA =
  (DESCRIPTION =
     (ADDRESS_LIST =
       (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC_FOR_XE))
     )
     (CONNECT_DATA =
       (SID = PLSExtProc)
       (PRESENTATION = RO)
     )
  )
" > $ORACLE_HOME/network/admin/tnsnames.ora

   su -p oracle -c "sqlplus / as sysdba <<EOF
      EXEC DBMS_XDB.SETLISTENERLOCALACCESS(FALSE);
      
      ALTER DATABASE ADD LOGFILE GROUP 4 ('$ORACLE_BASE/oradata/$ORACLE_SID/redo04.log') SIZE 10m;
      ALTER DATABASE ADD LOGFILE GROUP 5 ('$ORACLE_BASE/oradata/$ORACLE_SID/redo05.log') SIZE 10m;
      ALTER DATABASE ADD LOGFILE GROUP 6 ('$ORACLE_BASE/oradata/$ORACLE_SID/redo06.log') SIZE 10m;
      ALTER SYSTEM SWITCH LOGFILE;
      ALTER SYSTEM SWITCH LOGFILE;
      ALTER SYSTEM CHECKPOINT;
      ALTER DATABASE DROP LOGFILE GROUP 1;
      ALTER DATABASE DROP LOGFILE GROUP 2;
      
      ALTER SYSTEM SET db_recovery_file_dest='';
    --Drop Sample Schema in Users TS and delete the TS
    drop user hr cascade;
    drop tablespace users including contents and datafiles;

    -- Create Data Tablespace.
    create tablespace TAP_TS datafile '/u01/app/oracle/oradata/XE/TAP_TS.dbf' size 1M autoextend on next 1M maxsize 100M;

    -- Create a default APP User in your Database. Only if you want
    create user TAP_SCHEMA identified by TAP_SCHEMA default tablespace TAP_TS;
    GRANT ALTER SESSION TO TAP_SCHEMA;
    GRANT ANALYZE ANY TO TAP_SCHEMA;
    GRANT CREATE ANY SYNONYM TO TAP_SCHEMA;
    GRANT CREATE JOB TO TAP_SCHEMA;
    GRANT CREATE PROCEDURE TO TAP_SCHEMA;
    GRANT CREATE SEQUENCE TO TAP_SCHEMA;
    GRANT CREATE SESSION TO TAP_SCHEMA;
    GRANT CREATE SYNONYM TO TAP_SCHEMA;
    GRANT CREATE TABLE TO TAP_SCHEMA;
    GRANT CREATE TYPE TO TAP_SCHEMA;
    GRANT CREATE VIEW TO TAP_SCHEMA;
    GRANT CREATE TRIGGER TO TAP_SCHEMA;
    GRANT DEBUG CONNECT SESSION TO TAP_SCHEMA;
    alter user TAP_SCHEMA quota unlimited on TAP_TS;

    -- Create TAP SCHEMA
    --CREATE SCHEMA IF NOT EXISTS TAP_SCHEMA;

    -- minimal TAP_SCHEMA creation
    -- assumes that the TAP_SCHEMA schema exists
    -- sizes for fields are rather arbitrary and generous
    CREATE TABLE TAP_SCHEMA.schemas (
      schema_name varchar(64) NOT NULL,
      utype varchar(512),
      description varchar(512),
      PRIMARY KEY (schema_name)
    );

    CREATE TABLE TAP_SCHEMA.tables (
      table_name varchar(128) PRIMARY KEY,
      schema_name varchar(64) NOT NULL CONSTRAINT tables_schema_fk REFERENCES TAP_SCHEMA.schemas (schema_name),
      table_type varchar(8) NOT NULL,
      utype varchar(512),
      description varchar(512),
      table_index number(9)
    );

    CREATE TABLE TAP_SCHEMA.columns (
      table_name varchar(128) NOT NULL CONSTRAINT columns_tablename_fk REFERENCES TAP_SCHEMA.tables (table_name),
      column_name varchar(64) NOT NULL,
      utype varchar(512),
      ucd varchar(64),
      unit varchar(64),
      description varchar(512),
      datatype varchar(64) NOT NULL,
      arraysize number(9),
      principal number(9) NOT NULL,
      indexed number(9) NOT NULL,
      std number(9) NOT NULL,
      column_index number(9),
      id varchar(32),
      CONSTRAINT columns_pk PRIMARY KEY (table_name, column_name)
    );

    CREATE TABLE TAP_SCHEMA.keys (
      key_id varchar(64) NOT NULL PRIMARY KEY,
      from_table varchar(128) NOT NULL CONSTRAINT keys_from_table_fk REFERENCES TAP_SCHEMA.tables (table_name),
      target_table varchar(128) NOT NULL CONSTRAINT keys_target_table_fk REFERENCES TAP_SCHEMA.tables (table_name),
      utype varchar(512),
      description varchar(512)
    );

    CREATE TABLE TAP_SCHEMA.key_columns (
      key_id varchar(64) NOT NULL CONSTRAINT key_columns_keyid_fk REFERENCES TAP_SCHEMA.keys (key_id),
      from_column varchar(64) NOT NULL,
      target_column varchar(64) NOT NULL
    );

    --GRANT usage ON SCHEMA TAP_SCHEMA TO TAP_SCHEMA;
    --GRANT SELECT ON ALL tables IN SCHEMA TAP_SCHEMA TO TAP_SCHEMA;

    -- Insert TAP data

    -- content of the TAP_SCHEMA tables that describes the TAP_SCHEMA itself --

    -- delete key columns for keys from tables in the TAP_SCHEMA schema
    delete from TAP_SCHEMA.key_columns where key_id in (select key_id from TAP_SCHEMA.keys where from_table in
    (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA') or target_table in (select
    table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA'));

    -- delete keys from tables in the TAP_SCHEMA schema
    delete from TAP_SCHEMA.keys where
    from_table in (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA')
    or
    target_table in (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA');

    -- delete columns from tables in the TAP_SCHEMA schema
    delete from TAP_SCHEMA.columns where table_name in
    (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA');

    -- delete tables in the caom schema
    delete from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA';

    -- delete the caom schema
    delete from TAP_SCHEMA.schemas where upper(schema_name) = 'TAP_SCHEMA';

    insert into TAP_SCHEMA.schemas (schema_name,description,utype) values
    ( 'TAP_SCHEMA', 'a special schema to describe a TAP tableset', NULL );

    insert all
    into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
    ( 'TAP_SCHEMA', 'TAP_SCHEMA.schemas', 'table', 'description of schemas in this tableset', NULL, 1)
    into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
    ( 'TAP_SCHEMA', 'TAP_SCHEMA.tables', 'table', 'description of tables in this tableset', NULL, 2)
    into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
    ( 'TAP_SCHEMA', 'TAP_SCHEMA.columns', 'table', 'description of columns in this tableset', NULL, 3)
    into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
    ( 'TAP_SCHEMA', 'TAP_SCHEMA.keys', 'table', 'description of foreign keys in this tableset', NULL, 4)
    into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
    ( 'TAP_SCHEMA', 'TAP_SCHEMA.key_columns', 'table', 'description of foreign key columns in this tableset', NULL, 5)
    select 1 from dual;

    insert all
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.schemas', 'schema_name', 'schema name for reference to TAP_SCHEMA.schemas', NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.schemas', 'utype', 'lists the utypes of schemas in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,2 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.schemas', 'description', 'describes schemas in the tableset',               NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,3 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.tables', 'schema_name', 'the schema this table belongs to',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,1 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.tables', 'table_name', 'the fully qualified table name',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.tables', 'table_type', 'one of: table view',                                NULL, NULL, NULL, 'adql:VARCHAR', 8, 1,0,0,3 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.tables', 'utype', 'lists the utype of tables in the tableset',              NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,4 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.tables', 'description', 'describes tables in the tableset',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,5 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.tables', 'table_index', 'recommended sort order when listing tables',       NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,6 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'table_name', 'the table this column belongs to',                 NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'column_name', 'the column name',                                 NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'utype', 'lists the utypes of columns in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,3 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'ucd', 'lists the UCDs of columns in the tableset',               NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,4 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'unit', 'lists the unit used for column values in the tableset',  NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,5 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'description', 'describes the columns in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,6 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset', NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,7 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'arraysize', 'lists the size of variable-length columns in the tableset', NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,8 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', '"size"', 'deprecated: use arraysize', NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,9 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'principal', 'a principal column; 1 means 1, 0 means 0',      NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,10 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'indexed', 'an indexed column; 1 means 1, 0 means 0',         NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,11 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'std', 'a standard column; 1 means 1, 0 means 0',             NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,12 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.columns', 'column_index', 'recommended sort order when listing columns of a table',  NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,13 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.keys', 'key_id', 'unique key to join to TAP_SCHEMA.key_columns',            NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.keys', 'from_table', 'the table with the foreign key',                      NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.keys', 'target_table', 'the table with the primary key',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,3 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.keys', 'utype', 'lists the utype of keys in the tableset',              NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,4 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.keys', 'description', 'describes keys in the tableset',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,5 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.key_columns', 'key_id', 'key to join to TAP_SCHEMA.keys',                   NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.key_columns', 'from_column', 'column in the from_table',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
    into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
    ( 'TAP_SCHEMA.key_columns', 'target_column', 'column in the target_table',                NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,3 )
    select 1 from dual;

    insert all
    into TAP_SCHEMA.keys (key_id, from_table,target_table) values
    ( 'k1', 'TAP_SCHEMA.tables', 'TAP_SCHEMA.schemas' )
    into TAP_SCHEMA.keys (key_id, from_table,target_table) values
    ( 'k2', 'TAP_SCHEMA.columns', 'TAP_SCHEMA.tables' )
    into TAP_SCHEMA.keys (key_id, from_table,target_table) values
    ( 'k3', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables' )
    into TAP_SCHEMA.keys (key_id, from_table,target_table) values
    ( 'k4', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables' )
    into TAP_SCHEMA.keys (key_id, from_table,target_table) values
    ( 'k5', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.keys' )
    select 1 from dual;

    insert all
    into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
    ( 'k1', 'schema_name', 'schema_name' )
    into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
    ( 'k2', 'table_name', 'table_name' )
    into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
    ( 'k3', 'from_table', 'table_name' )
    into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
    ( 'k4', 'target_table', 'table_name' )
    into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
    ( 'k5', 'key_id', 'key_id' )
    select 1 from dual;

    -- backwards compatible: fill size column with values from arraysize set above
    -- update TAP_SCHEMA.columns SET "size" = arraysize WHERE table_name LIKE 'TAP_SCHEMA.%';


    -- Shrink Temp / SYSTEM Tablespace
    alter database tempfile '/u01/app/oracle/oradata/XE/temp.dbf' resize 10M;
    alter database datafile '/u01/app/oracle/oradata/XE/system.dbf' resize 353M;

    -- Remove Apex
    @/u01/app/oracle/product/11.2.0/xe/apex/apxremov.sql
    drop package HTMLDB_SYSTEM;

    -- Remove XDB
    shutdown immediate
    startup upgrade;
    @/u01/app/oracle/product/11.2.0/xe/rdbms/admin/catnoqm.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/catxdbdv.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/dbmsmeta.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/dbmsmeti.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/dbmsmetu.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/dbmsmetb.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/dbmsmetd.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/dbmsmet2.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/catmeta.sql
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/prvtmeta.plb
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/prvtmeti.plb
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/prvtmetu.plb
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/prvtmetb.plb
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/prvtmetd.plb
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/prvtmet2.plb
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/catmet2.sql

    -- Remove Text
    @/u01/app/oracle/product/11.2.0/xe/ctx/admin/catnoctx.sql
    drop procedure sys.validate_context;
    drop user MDSYS cascade;
    start /u01/app/oracle/product/11.2.0/xe/rdbms/admin/utlrp.sql

    -- After removing TEXT and APEX from the DB we can remove it from the Home
    host rm -rf /u01/app/oracle/product/11.2.0/xe/apex
    host rm -rf /u01/app/oracle/product/11.2.0/xe/ctx

    -- Create a Smaller UNDO. We add a new one and drop the old
    create undo tablespace undotbs2 datafile '/u01/app/oracle/oradata/XE/undotbs2.dbf' size 1M autoextend on next 10M maxsize 1G;
    alter system set undo_tablespace='undotbs2';
    shutdown immediate;
    startup
    drop tablespace undotbs1 including contents and datafiles;
    shutdown immediate;
    exit;
      exit;
EOF"

  # Move database operational files to oradata
  moveFiles;
}

############# MAIN ################

# Set SIGTERM handler
trap _term SIGTERM

# Set SIGKILL handler
trap _kill SIGKILL

# Check whether database already exists
if [ -d $ORACLE_BASE/oradata/$ORACLE_SID ]; then
   symLinkFiles;
   # Make sure audit file destination exists
   if [ ! -d $ORACLE_BASE/admin/$ORACLE_SID/adump ]; then
      su -p oracle -c "mkdir -p $ORACLE_BASE/admin/$ORACLE_SID/adump"
   fi;
fi;

/etc/init.d/oracle-xe start | grep -qc "Oracle Database 11g Express Edition is not configured"
if [ "$?" == "0" ]; then
   # Check whether container has enough memory
   if [ `df -Pk /dev/shm | tail -n 1 | awk '{print $2}'` -lt 1048576 ]; then
      echo "Error: The container doesn't have enough memory allocated."
      echo "A database XE container needs at least 1 GB of shared memory (/dev/shm)."
      echo "You currently only have $((`df -Pk /dev/shm | tail -n 1 | awk '{print $2}'`/1024)) MB allocated to the container."
      exit 1;
   fi;
   
   # Create database
   createDB;
   
   # Execute custom provided setup scripts
   runUserScripts $ORACLE_BASE/scripts/setup
fi;

# Check whether database is up and running
$ORACLE_BASE/$CHECK_DB_FILE
if [ $? -eq 0 ]; then
  echo "#########################"
  echo "DATABASE IS READY TO USE!"
  echo "#########################"

  # Execute custom provided startup scripts
  #runUserScripts $ORACLE_BASE/scripts/startup

else
  echo "#####################################"
  echo "########### E R R O R ###############"
  echo "DATABASE SETUP WAS NOT SUCCESSFUL!"
  echo "Please check output for further info!"
  echo "########### E R R O R ###############"
  echo "#####################################"
fi;

#echo "The following output is now a tail of the alert.log:"
#tail -f $ORACLE_BASE/diag/rdbms/*/*/trace/alert*.log &
#childPID=$!
#wait $childPID
