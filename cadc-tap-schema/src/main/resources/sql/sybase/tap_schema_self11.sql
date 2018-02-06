
-- content of the TAP_SCHEMA tables that describes the TAP_SCHEMA itself --

-- note: this makes use of the multiple insert support in PostgreSQL and
-- may not be portable

-- delete key columns for keys from tables in the TAP_SCHEMA schema
delete from tap_schema_key_columns11 where
key_id in (select key_id from tap_schema_keys11 where
    from_table in (select table_name from tap_schema_tables11 where schema_name = 'tap_schema')
    or
    target_table in (select table_name from tap_schema_tables11 where schema_name = 'tap_schema')
)
;

-- delete keys from tables in the TAP_SCHEMA schema
delete from tap_schema_keys11 where
from_table in (select table_name from tap_schema_tables11 where schema_name = 'tap_schema')
or
target_table in (select table_name from tap_schema_tables11 where schema_name = 'tap_schema')
;

-- delete columns from tables in the TAP_SCHEMA schema
delete from tap_schema_columns11 where table_name in
(select table_name from tap_schema_tables11 where schema_name = 'tap_schema')
;

-- delete tables in the caom schema
delete from tap_schema_tables11 where schema_name = 'tap_schema';

-- delete the caom schema
delete from tap_schema_schemas11 where schema_name = 'tap_schema';


insert into tap_schema_schemas11 (schema_name,description,utype) values( 'tap_schema', 'standard schema to describe a TAP tableset', NULL );

insert into tap_schema_tables11 (schema_name,table_name,table_type,description,utype,table_index) 
values ( 'tap_schema', 'tap_schema.schemas', 'table', 'description of schemas in this tableset', NULL,1 );
insert into tap_schema_tables11 (schema_name,table_name,table_type,description,utype,table_index) 
values ( 'tap_schema', 'tap_schema.tables', 'table', 'description of tables in this tableset', NULL,2 );
insert into tap_schema_tables11 (schema_name,table_name,table_type,description,utype,table_index) 
values ( 'tap_schema', 'tap_schema.columns', 'table', 'description of columns in this tableset', NULL,3 );
insert into tap_schema_tables11 (schema_name,table_name,table_type,description,utype,table_index) 
values ( 'tap_schema', 'tap_schema.keys', 'table', 'description of foreign keys in this tableset', NULL,4 );
insert into tap_schema_tables11 (schema_name,table_name,table_type,description,utype,table_index) 
values ( 'tap_schema', 'tap_schema.key_columns', 'table', 'description of foreign key columns in this tableset', NULL,5 );

insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.schemas', 'schema_name', 'schema name for reference to schemas',                 NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,1 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.schemas', 'utype', 'lists the utypes of schemas in the tableset',                NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,2 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.schemas', 'description', 'describes schemas in the tableset',                    NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,3 );

insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.tables', 'schema_name', 'the schema this table belongs to',                      NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,1 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.tables', 'table_name', 'the fully qualified table name',                         NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,2 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.tables', 'table_type', 'one of: table view',                                     NULL, NULL, NULL, 'char', '8*', 8, NULL, 1,0,0,3 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.tables', 'utype', 'lists the utype of tables in the tableset',                   NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,4 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.tables', 'description', 'describes tables in the tableset',                      NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,5 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'table_name', 'the table this column belongs to',                      NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,6 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'column_name', 'the column name',                                      NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,7 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'utype', 'lists the utypes of columns in the tableset',                NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,8 )

insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'ucd', 'lists the UCDs of columns in the tableset',                    NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,1 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'unit', 'lists the unit used for column values in the tableset',       NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,2 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'description', 'describes the columns in the tableset',                NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,3 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset',      NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,4 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'arraysize', 'lists the size of variable-length columns in the tableset',   NULL, NULL, NULL, 'char', '16*', NULL, NULL, 1,0,0,5 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'xtype', 'lists the size of variable-length columns in the tableset',   NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,5 );

insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', '"size"', 'deprecated: use arraysize',   NULL, NULL, NULL, 'int', NULL, NULL, NULL, 1,0,0,6 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'principal', 'a principal column; 1 means 1, 0 means 0',               NULL, NULL, NULL, 'int', NULL, NULL, NULL, 1,0,0,7 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'indexed', 'an indexed column; 1 means 1, 0 means 0',                  NULL, NULL, NULL, 'int', NULL, NULL, NULL, 1,0,0,8 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.columns', 'std', 'a standard column; 1 means 1, 0 means 0',                      NULL, NULL, NULL, 'int', NULL, NULL, NULL, 1,0,0,9 );

insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.keys', 'key_id', 'unique key to join to key_columns',                            NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,1 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.keys', 'from_table', 'the table with the foreign key',                           NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,2 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.keys', 'target_table', 'the table with the primary key',                         NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,3 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.keys', 'utype', 'lists the utype of keys in the tableset',                       NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,4 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.keys', 'description', 'describes keys in the tableset',                          NULL, NULL, NULL, 'char', '512*', 512, NULL, 1,0,0,5 );

insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.key_columns', 'key_id', 'key to join to keys',                                   NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,1 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.key_columns', 'from_column', 'column in the from_table',                         NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,2 );
insert into tap_schema_columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,size,xtype,principal,indexed,std,column_index) 
values ( 'tap_schema.key_columns', 'target_column', 'column in the target_table',                     NULL, NULL, NULL, 'char', '64*', 64, NULL, 1,0,0,3 );


insert into tap_schema_keys11 (key_id, from_table,target_table) values ( 'k1', 'tap_schema.tables', 'tap_schema.schemas' );
insert into tap_schema_keys11 (key_id, from_table,target_table) values ( 'k2', 'tap_schema.columns', 'tap_schema.tables' );
insert into tap_schema_keys11 (key_id, from_table,target_table) values ( 'k3', 'tap_schema.keys', 'tap_schema.tables' );     -- two separate foreign keys: see below
insert into tap_schema_keys11 (key_id, from_table,target_table) values ( 'k4', 'tap_schema.keys', 'tap_schema.tables' );     -- two separate foreign keys: see below
insert into tap_schema_keys11 (key_id, from_table,target_table) values ( 'k5', 'tap_schema.key_columns', 'tap_schema.keys' );

insert into tap_schema_key_columns11 (key_id,from_column,target_column) values ( 'k1', 'schema_name', 'schema_name' );
insert into tap_schema_key_columns11 (key_id,from_column,target_column) values ( 'k2', 'table_name', 'table_name' );
insert into tap_schema_key_columns11 (key_id,from_column,target_column) values ( 'k3', 'from_table', 'table_name' );
insert into tap_schema_key_columns11 (key_id,from_column,target_column) values ( 'k4', 'target_table', 'table_name' );
insert into tap_schema_key_columns11 (key_id,from_column,target_column) values ( 'k5', 'key_id', 'key_id' );

