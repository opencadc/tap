
-- content of the tap_schema tables that describes the tap_schema itself
-- the 11 suffix on all physical table names means this is the TAP-1.1 version
-- as required by the cadc-tap-schema library

-- delete key columns for keys from tables in the tap_schema schema
delete from tap_schema.key_columns11 where
key_id in (select key_id from tap_schema.keys11 where 
    from_table in   (select table_name from tap_schema.tables11 where lower(table_name) like 'tap_schema.%')
    or
    target_table in (select table_name from tap_schema.tables11 where lower(table_name) like 'tap_schema.%')
)
;

-- delete keys from tables in the tap_schema schema
delete from tap_schema.keys11 where 
from_table in   (select table_name from tap_schema.tables11 where lower(table_name) like 'tap_schema.%')
or
target_table in (select table_name from tap_schema.tables11 where lower(table_name) like 'tap_schema.%')
;

-- delete columns from tables in the tap_schema schema
delete from tap_schema.columns11 where table_name in 
(select table_name from tap_schema.tables11 where lower(table_name) like 'tap_schema.%')
;

-- delete tables
delete from tap_schema.tables11 where lower(table_name) like 'tap_schema.%'
;

-- delete schema
delete from tap_schema.schemas11 where lower(schema_name) = 'tap_schema'
;


insert into tap_schema.schemas11 (schema_name,description,utype) values
( 'tap_schema', 'a special schema to describe TAP-1.1 tablesets', NULL )
;

insert into tap_schema.tables11 (schema_name,table_name,table_type,description,utype,table_index) values
( 'tap_schema', 'tap_schema.schemas', 'table', 'description of schemas in this tableset', NULL, 1),
( 'tap_schema', 'tap_schema.tables', 'table', 'description of tables in this tableset', NULL, 2),
( 'tap_schema', 'tap_schema.columns', 'table', 'description of columns in this tableset', NULL, 3),
( 'tap_schema', 'tap_schema.keys', 'table', 'description of foreign keys in this tableset', NULL, 4),
( 'tap_schema', 'tap_schema.key_columns', 'table', 'description of foreign key columns in this tableset', NULL, 5)
;

insert into tap_schema.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'tap_schema.schemas', 'schema_name', 'schema name for reference to tap_schema.schemas', NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'tap_schema.schemas', 'utype', 'lists the utypes of schemas in the tableset',           NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,2 ),
( 'tap_schema.schemas', 'description', 'describes schemas in the tableset',               NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,3 ),
( 'tap_schema.schemas', 'schema_index', 'recommended sort order when listing schemas',    NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,4 ),

( 'tap_schema.tables', 'schema_name', 'the schema this table belongs to',                 NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,1 ),
( 'tap_schema.tables', 'table_name', 'the fully qualified table name',                    NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'tap_schema.tables', 'table_type', 'one of: table view',                                NULL, NULL, NULL, 'char', '8*', NULL, 1,0,1,3 ),
( 'tap_schema.tables', 'utype', 'lists the utype of tables in the tableset',              NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,4 ),
( 'tap_schema.tables', 'description', 'describes tables in the tableset',                 NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,5 ),
( 'tap_schema.tables', 'table_index', 'recommended sort order when listing tables',       NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,6 ),

( 'tap_schema.columns', 'table_name', 'the table this column belongs to',                 NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'tap_schema.columns', 'column_name', 'the column name',                                 NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'tap_schema.columns', 'utype', 'lists the utypes of columns in the tableset',           NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,3 ),
( 'tap_schema.columns', 'ucd', 'lists the UCDs of columns in the tableset',               NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,4 ),
( 'tap_schema.columns', 'unit', 'lists the unit used for column values in the tableset',  NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,5 ),
( 'tap_schema.columns', 'description', 'describes the columns in the tableset',           NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,6 ),
( 'tap_schema.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset', NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,7 ),
( 'tap_schema.columns', 'arraysize', 'lists the size of variable-length columns in the tableset', NULL, NULL, NULL, 'char', '16*', NULL, 1,0,1,8 ),
( 'tap_schema.columns', 'xtype', 'a DALI or custom extended type annotation',             NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,7 ),

( 'tap_schema.columns', '"size"', 'deprecated: use arraysize', NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,9 ),
( 'tap_schema.columns', 'principal', 'a principal column; 1 means true, 0 means false',      NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,10 ),
( 'tap_schema.columns', 'indexed', 'an indexed column; 1 means true, 0 means false',         NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,11 ),
( 'tap_schema.columns', 'std', 'a standard column; 1 means true, 0 means false',             NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,12 ),
( 'tap_schema.columns', 'column_index', 'recommended sort order when listing columns of a table',  NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,13 ),

( 'tap_schema.keys', 'key_id', 'unique key to join to tap_schema.key_columns',            NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'tap_schema.keys', 'from_table', 'the table with the foreign key',                      NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'tap_schema.keys', 'target_table', 'the table with the primary key',                    NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,3 ),
( 'tap_schema.keys', 'utype', 'lists the utype of keys in the tableset',              NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,4 ),
( 'tap_schema.keys', 'description', 'describes keys in the tableset',                 NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,5 ),

( 'tap_schema.key_columns', 'key_id', 'key to join to tap_schema.keys',                   NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'tap_schema.key_columns', 'from_column', 'column in the from_table',                    NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'tap_schema.key_columns', 'target_column', 'column in the target_table',                NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,3 )
;

insert into tap_schema.keys11 (key_id,from_table,target_table) values
( 'k1', 'tap_schema.tables', 'tap_schema.schemas' ),
( 'k2', 'tap_schema.columns', 'tap_schema.tables' ), 
( 'k3', 'tap_schema.keys', 'tap_schema.tables' ),
( 'k4', 'tap_schema.keys', 'tap_schema.tables' ),
( 'k5', 'tap_schema.key_columns', 'tap_schema.keys' ),
( 'k6', 'tap_schema.key_columns', 'tap_schema.columns' ),
( 'k7', 'tap_schema.key_columns', 'tap_schema.columns' )
;

insert into tap_schema.key_columns11 (key_id,from_column,target_column) values
( 'k1', 'schema_name', 'schema_name' ),
( 'k2', 'table_name', 'table_name' ),
( 'k3', 'from_table', 'table_name' ),
( 'k4', 'target_table', 'table_name' ),
( 'k5', 'key_id', 'key_id' ),
( 'k6', 'from_column', 'column_name'),
( 'k7', 'target_column', 'column_name')
;

-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
update tap_schema.columns11 SET "size" = replace(arraysize::varchar,'*','')::int 
WHERE table_name LIKE 'tap_schema.%'
  AND arraysize IS NOT NULL
  AND arraysize NOT LIKE '%x%'
  AND arraysize != '*';

