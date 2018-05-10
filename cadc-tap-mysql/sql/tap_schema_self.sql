 # content of the TAP_SCHEMA tables that describes the TAP_SCHEMA itself --

 # note: this makes use of the multiple insert support in PostgreSQL and
 # may not be portable

 # delete key columns for keys from tables in the TAP_SCHEMA schema
delete from TAP.key_columns where
key_id in (select key_id from TAP.keys where 
    from_table in (select table_name from TAP.tables where upper(schema_name) = 'TAP_SCHEMA')
    or
    target_table in (select table_name from TAP.tables where upper(schema_name) = 'TAP_SCHEMA')
)
;

 # delete keys from tables in the TAP_SCHEMA schema
delete from TAP.keys where 
from_table in (select table_name from TAP.tables where upper(schema_name) = 'TAP_SCHEMA')
or
target_table in (select table_name from TAP.tables where upper(schema_name) = 'TAP_SCHEMA')
;

 # delete columns from tables in the TAP_SCHEMA schema
delete from TAP.columns where table_name in 
(select table_name from TAP.tables where upper(schema_name) = 'TAP_SCHEMA')
;

 # delete tables in the caom schema
delete from TAP.tables where upper(schema_name) = 'TAP_SCHEMA'
;

 # delete the caom schema
delete from TAP.schemas where upper(schema_name) = 'TAP_SCHEMA'
;


insert into TAP.schemas (schema_name,description,utype) values
( 'TAP_SCHEMA', 'a special schema to describe a TAP tableset', NULL )
;

insert into TAP.tables (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP.schemas', 'table', 'description of schemas in this tableset', NULL, 1),
( 'TAP_SCHEMA', 'TAP.tables', 'table', 'description of tables in this tableset', NULL, 2),
( 'TAP_SCHEMA', 'TAP.columns', 'table', 'description of columns in this tableset', NULL, 3),
( 'TAP_SCHEMA', 'TAP.keys', 'table', 'description of foreign keys in this tableset', NULL, 4),
( 'TAP_SCHEMA', 'TAP.key_columns', 'table', 'description of foreign key columns in this tableset', NULL, 5)
;

insert into TAP.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP.schemas', 'schema_name', 'schema name for reference to TAP.schemas', NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 ),
( 'TAP.schemas', 'utype', 'lists the utypes of schemas in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,2 ),
( 'TAP.schemas', 'description', 'describes schemas in the tableset',               NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,3 ),

( 'TAP.tables', 'schema_name', 'the schema this table belongs to',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,1 ),
( 'TAP.tables', 'table_name', 'the fully qualified table name',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 ),
( 'TAP.tables', 'table_type', 'one of: table view',                                NULL, NULL, NULL, 'adql:VARCHAR', 8, 1,0,0,3 ),
( 'TAP.tables', 'utype', 'lists the utype of tables in the tableset',              NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,4 ),
( 'TAP.tables', 'description', 'describes tables in the tableset',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,5 ),
( 'TAP.tables', 'table_index', 'recommended sort order when listing tables',       NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,6 ),

( 'TAP.columns', 'table_name', 'the table this column belongs to',                 NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 ),
( 'TAP.columns', 'column_name', 'the column name',                                 NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 ),
( 'TAP.columns', 'utype', 'lists the utypes of columns in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,3 ),
( 'TAP.columns', 'ucd', 'lists the UCDs of columns in the tableset',               NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,4 ),
( 'TAP.columns', 'unit', 'lists the unit used for column values in the tableset',  NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,5 ),
( 'TAP.columns', 'description', 'describes the columns in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,6 ),
( 'TAP.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset', NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,7 ),
( 'TAP.columns', 'arraysize', 'lists the size of variable-length columns in the tableset', NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,8 ),
( 'TAP.columns', '"size"', 'deprecated: use arraysize', NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,9 ),
( 'TAP.columns', 'principal', 'a principal column; 1 means 1, 0 means 0',      NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,10 ),
( 'TAP.columns', 'indexed', 'an indexed column; 1 means 1, 0 means 0',         NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,11 ),
( 'TAP.columns', 'std', 'a standard column; 1 means 1, 0 means 0',             NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,12 ),
( 'TAP.columns', 'column_index', 'recommended sort order when listing columns of a table',  NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,13 ),

( 'TAP.keys', 'key_id', 'unique key to join to TAP.key_columns',            NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 ),
( 'TAP.keys', 'from_table', 'the table with the foreign key',                      NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 ),
( 'TAP.keys', 'target_table', 'the table with the primary key',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,3 ),
( 'TAP.keys', 'utype', 'lists the utype of keys in the tableset',              NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,4 ),
( 'TAP.keys', 'description', 'describes keys in the tableset',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,5 ),

( 'TAP.key_columns', 'key_id', 'key to join to TAP.keys',                   NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 ),
( 'TAP.key_columns', 'from_column', 'column in the from_table',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 ),
( 'TAP.key_columns', 'target_column', 'column in the target_table',                NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,3 )
;

insert into TAP.keys (key_id, from_table,target_table) values
( 'k1', 'TAP.tables', 'TAP.schemas' ),
( 'k2', 'TAP.columns', 'TAP.tables' ), 
( 'k3', 'TAP.keys', 'TAP.tables' ),     -- two separate foreign keys: see below
( 'k4', 'TAP.keys', 'TAP.tables' ),     -- two separate foreign keys: see below
( 'k5', 'TAP.key_columns', 'TAP.keys' )
;

insert into TAP.key_columns (key_id,from_column,target_column) values
( 'k1', 'schema_name', 'schema_name' ),
( 'k2', 'table_name', 'table_name' ),
( 'k3', 'from_table', 'table_name' ),
( 'k4', 'target_table', 'table_name' ),
( 'k5', 'key_id', 'key_id' )
;

 # backwards compatible: fill "size" column with values from arraysize set above
update TAP.columns SET size = arraysize WHERE table_name LIKE 'TAP.%';
