
-- cleanup TEST description
delete from TAP_SCHEMA.columns where table_name = 'tap_test.AllDataTypes';
delete from TAP_SCHEMA.tables where table_name = 'tap_test.AllDataTypes';

insert into TAP_SCHEMA.tables (schema_name,table_name,description) values
( 'tap_test', 'tap_test.AllDataTypes', 'sample table with all current internal datatypes and one row of values');

insert into TAP_SCHEMA.columns (table_name,column_name,description,ucd,unit,datatype,arraysize,principal,indexed,std) values
( 'tap_test.AllDataTypes', 't_char', 'char or char(1)',  NULL, NULL, 'adql:CHAR', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_clob', 'clob',  NULL, NULL, 'adql:CLOB', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_char_n', 'char(n)',  NULL, NULL, 'adql:CHAR', 8, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_string', 'string',  NULL, NULL, 'adql:VARCHAR', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_string_n', 'string',  NULL, NULL, 'adql:VARCHAR', 64, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_unsignedByte_1', 'binary(1)',  NULL, NULL, 'adql:BINARY', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_blob', 'blob',  NULL, NULL, 'adql:BLOB', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_unsignedByte_n', 'binary(n)',  NULL, NULL, 'adql:BINARY', 16, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_unsignedByte_n_any', 'varbinary(n*)',  NULL, NULL, 'adql:VARBINARY', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_short', 'smallint',  NULL, NULL, 'adql:SMALLINT', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_int', 'integer (32-bit)',  NULL, NULL, 'adql:INTEGER', 1, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_long', 'long (64-bit)',  NULL, NULL, 'adql:BIGINT', 1, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_float', 'real',  NULL, NULL, 'adql:REAL', 1, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_double', 'double (64-bit)',  NULL, NULL, 'adql:DOUBLE', 1, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_date', 'Date',  NULL, NULL, 'adql:TIMESTAMP', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_poly_char', 'TAP 1.0 region in characters', NULL, NULL, 'adql:REGION', NULL, 1,0,0 ),
( 'tap_test.AllDataTypes', 't_point_char', 'TAP 1.0 point in characters', NULL, NULL, 'adql:POINT', NULL, 1,0,0 )
;

-- compatibility with TAP-1.0: fill "size" column with arraysize values
update tap_test.AllDataTypes set size = arraysize where table_name like 'tap_test.%';

