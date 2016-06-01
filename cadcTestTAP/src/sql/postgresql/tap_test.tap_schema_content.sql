
-- cleanup
-- cleanup
delete from TAP_SCHEMA.columns where table_name in (select table_name from tap_schema.tables where schema_name = 'tap_test');

delete from TAP_SCHEMA.tables where schema_name = 'tap_test';

delete from TAP_SCHEMA.schemas where schema_name = 'tap_test';

insert into TAP_SCHEMA.schemas (schema_name,description) values
( 'tap_test', 'Tables for the client and service testing');

insert into TAP_SCHEMA.tables (schema_name,table_name,description) values
( 'tap_test', 'tap_test.AllDataTypes', 'sample table with all current internal datatypes and sample values');

insert into TAP_SCHEMA.columns (table_name,column_name,description,ucd,unit,datatype,arraysize,principal,indexed,std) values
( 'tap_test.AllDataTypes', 't_char', 'char or char(1)',  NULL, NULL, 'adql:CHAR',                NULL,  1,0,0 ),
( 'tap_test.AllDataTypes', 't_clob', 'clob',  NULL, NULL, 'adql:CLOB',                           NULL,  1,0,0 ),
( 'tap_test.AllDataTypes', 't_char_n', 'char(n)',  NULL, NULL, 'adql:CHAR',                         8,  1,0,0 ),
( 'tap_test.AllDataTypes', 't_varchar_n', 'string',  NULL, NULL, 'adql:VARCHAR',                   64,  1,0,0 ),
( 'tap_test.AllDataTypes', 't_varchar', 'string',  NULL, NULL, 'adql:VARCHAR',                    NULL, 1,0,0 ),

( 'tap_test.AllDataTypes', 't_byte', 'binary(1)',  NULL, NULL, 'adql:BINARY',                     NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_blob', 'blob',  NULL, NULL, 'adql:BLOB',                            NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_binary_n', 'binary(n)',  NULL, NULL, 'adql:BINARY',                   16, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_varbinary_n', 'varbinary(16)',  NULL, NULL, 'adql:VARBINARY',         16, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_varbinary', 'varbinary(*)',  NULL, NULL, 'adql:VARBINARY',          NULL, 1,0,0 ),

( 'tap_test.AllDataTypes', 't_short', 'smallint',  NULL, NULL, 'adql:SMALLINT',                   NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_int', 'integer (32-bit)',  NULL, NULL, 'adql:INTEGER',              NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_long', 'long (64-bit)',  NULL, NULL, 'adql:BIGINT',                 NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_float', 'real',  NULL, NULL, 'adql:REAL',                           NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_double', 'double (64-bit)',  NULL, NULL, 'adql:DOUBLE',             NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_timestamp', 'both date and time',  NULL, NULL, 'adql:TIMESTAMP',    NULL, 1,0,0 ),

( 'tap_test.AllDataTypes', 't_stc_point', 'TAP 1.0 STC point', NULL, NULL, 'adql:POINT',          NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_stc_polygon', 'TAP 1.0 STC polygon', NULL, NULL, 'adql:REGION',     NULL, 1,0,0 )
;

-- compatibility with TAP-1.0: fill "size" column with arraysize values
update tap_schema.columns set size = arraysize where table_name like 'tap_test.%';

