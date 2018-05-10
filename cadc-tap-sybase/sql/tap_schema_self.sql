-- content of the TAP_SCHEMA tables that describes the TAP_SCHEMA itself --

-- note: this makes use of the multiple insert support in PostgreSQL and
-- may not be portable

-- delete key columns for keys from tables in the TAP_SCHEMA schema
delete from TAP_SCHEMA.key_columns where
key_id in (select key_id from TAP_SCHEMA.keys where 
    from_table in (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA')
    or
    target_table in (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA')
)
go

-- delete keys from tables in the TAP_SCHEMA schema
delete from TAP_SCHEMA.keys where 
from_table in (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA')
or
target_table in (select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA')
go

-- delete columns from tables in the TAP_SCHEMA schema
delete from TAP_SCHEMA.columns where table_name in 
(select table_name from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA')
go

-- delete tables in the caom schema
delete from TAP_SCHEMA.tables where upper(schema_name) = 'TAP_SCHEMA'
go

-- delete the caom schema
delete from TAP_SCHEMA.schemas where upper(schema_name) = 'TAP_SCHEMA'
go


insert into TAP_SCHEMA.schemas (schema_name,description,utype) values
( 'TAP_SCHEMA', 'a special schema to describe a TAP tableset', NULL )
go

insert into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP_SCHEMA.schemas', 'table', 'description of schemas in this tableset', NULL, 1)
go

insert into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP_SCHEMA.tables', 'table', 'description of tables in this tableset', NULL, 2)
go

insert into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP_SCHEMA.columns', 'table', 'description of columns in this tableset', NULL, 3)
go

insert into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP_SCHEMA.keys', 'table', 'description of foreign keys in this tableset', NULL, 4)
to

insert into TAP_SCHEMA.tables (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP_SCHEMA.key_columns', 'table', 'description of foreign key columns in this tableset', NULL, 5)
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.schemas', 'schema_name', 'schema name for reference to TAP_SCHEMA.schemas', NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.schemas', 'utype', 'lists the utypes of schemas in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,2 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.schemas', 'description', 'describes schemas in the tableset',               NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,3 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.tables', 'schema_name', 'the schema this table belongs to',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,1 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.tables', 'table_name', 'the fully qualified table name',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.tables', 'table_type', 'one of: table view',                                NULL, NULL, NULL, 'adql:VARCHAR', 8, 1,0,0,3 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.tables', 'utype', 'lists the utype of tables in the tableset',              NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,4 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.tables', 'description', 'describes tables in the tableset',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,5 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.tables', 'table_index', 'recommended sort order when listing tables',       NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,6 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'table_name', 'the table this column belongs to',                 NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'column_name', 'the column name',                                 NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'utype', 'lists the utypes of columns in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,3 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'ucd', 'lists the UCDs of columns in the tableset',               NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,4 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'unit', 'lists the unit used for column values in the tableset',  NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,5 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'description', 'describes the columns in the tableset',           NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,6 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset', NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,7 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'arraysize', 'lists the size of variable-length columns in the tableset', NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,8 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', '"size"', 'deprecated: use arraysize', NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,9 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'principal', 'a principal columngo 1 means 1, 0 means 0',      NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,10 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'indexed', 'an indexed columngo 1 means 1, 0 means 0',         NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,11 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'std', 'a standard columngo 1 means 1, 0 means 0',             NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,12 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.columns', 'column_index', 'recommended sort order when listing columns of a table',  NULL, NULL, NULL, 'adql:INTEGER', NULL, 1,0,0,13 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.keys', 'key_id', 'unique key to join to TAP_SCHEMA.key_columns',            NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.keys', 'from_table', 'the table with the foreign key',                      NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.keys', 'target_table', 'the table with the primary key',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,3 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.keys', 'utype', 'lists the utype of keys in the tableset',              NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,4 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.keys', 'description', 'describes keys in the tableset',                 NULL, NULL, NULL, 'adql:VARCHAR', 512, 1,0,0,5 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.key_columns', 'key_id', 'key to join to TAP_SCHEMA.keys',                   NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,1 )
go
insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.key_columns', 'from_column', 'column in the from_table',                    NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,2 )
go

insert into TAP_SCHEMA.columns (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.key_columns', 'target_column', 'column in the target_table',                NULL, NULL, NULL, 'adql:VARCHAR', 64, 1,0,0,3 )
go

insert into TAP_SCHEMA.keys (key_id, from_table,target_table) values
( 'k1', 'TAP_SCHEMA.tables', 'TAP_SCHEMA.schemas' )
go
insert into TAP_SCHEMA.keys (key_id, from_table,target_table) values
( 'k2', 'TAP_SCHEMA.columns', 'TAP_SCHEMA.tables' )
go
insert into TAP_SCHEMA.keys (key_id, from_table,target_table) values
( 'k3', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables' )     -- two separate foreign keys: see below
go
insert into TAP_SCHEMA.keys (key_id, from_table,target_table) values
( 'k4', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables' )     -- two separate foreign keys: see below
go

insert into TAP_SCHEMA.keys (key_id, from_table,target_table) values
( 'k5', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.keys' )
go

insert into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
( 'k1', 'schema_name', 'schema_name' )
go
insert into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
( 'k2', 'table_name', 'table_name' )
go
insert into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
( 'k3', 'from_table', 'table_name' )
go
insert into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
( 'k4', 'target_table', 'table_name' )
go
insert into TAP_SCHEMA.key_columns (key_id,from_column,target_column) values
( 'k5', 'key_id', 'key_id' )
go

-- backwards compatible: fill "size" column with values from arraysize set above
update TAP_SCHEMA.columns SET "size" = arraysize WHERE table_name LIKE 'TAP_SCHEMA.%'go
