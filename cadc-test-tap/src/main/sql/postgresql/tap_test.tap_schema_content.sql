
-- cleanup
-- cleanup
delete from TAP_SCHEMA.columns11 where table_name in (select table_name from tap_schema.tables11 where schema_name = 'tap_test');

delete from TAP_SCHEMA.tables11 where schema_name = 'tap_test';

delete from TAP_SCHEMA.schemas11 where schema_name = 'tap_test';

insert into TAP_SCHEMA.schemas11 (schema_name,description) values
( 'tap_test', 'Tables for the client and service testing');

insert into TAP_SCHEMA.tables11 (schema_name,table_name,table_type,description) values
( 'tap_test', 'tap_test.AllDataTypes', 'table', 'sample table with all current internal datatypes and sample values');

insert into TAP_SCHEMA.columns11 (table_name,column_name,description,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'tap_test.AllDataTypes', 't_char', 'char or char(1)',     'char', NULL, NULL,     1,0,0 ,1),
( 'tap_test.AllDataTypes', 't_clob', 'clob',  'char',       '*', 'clob',            1,0,0 ,2),
( 'tap_test.AllDataTypes', 't_char_n', 'char(n)',           'char', '8', NULL,      1,0,0 ,3),
( 'tap_test.AllDataTypes', 't_varchar_n', 'string',         'char', '64*', NULL,    1,0,0 ,4),
( 'tap_test.AllDataTypes', 't_varchar', 'string',           'char', '*', NULL,      1,0,0 ,5),

( 'tap_test.AllDataTypes', 't_blob', 'blob',                'unsignedByte', '*', 'blob',    1,0,0 ,11),
( 'tap_test.AllDataTypes', 't_binary_n', 'binary(n)',       'unsignedByte', '16', NULL,     1,0,0 ,12),
( 'tap_test.AllDataTypes', 't_varbinary_n', 'varbinary(16)','unsignedByte', '16*', NULL,    1,0,0 ,13),
( 'tap_test.AllDataTypes', 't_varbinary', 'varbinary(*)',   'unsignedByte', '*', NULL,      1,0,0 ,14),

( 'tap_test.AllDataTypes', 't_short', 'smallint',           'short', NULL, NULL,    1,0,0 ,20),
( 'tap_test.AllDataTypes', 't_int', 'integer (32-bit)',     'int', NULL, NULL,      1,0,0 ,21),
( 'tap_test.AllDataTypes', 't_long', 'long (64-bit)',       'long', NULL, NULL,     1,0,0 ,22),
( 'tap_test.AllDataTypes', 't_float', 'real',               'float', NULL, NULL,    1,0,0 ,23),
( 'tap_test.AllDataTypes', 't_double', 'double (64-bit)',   'double', NULL, NULL,   1,0,0 ,24),

( 'tap_test.AllDataTypes', 't_adql_timestamp', 'TAP-1.0 timestamp',      'char', '*', 'adql:TIMESTAMP',   1,0,0 ,30),
( 'tap_test.AllDataTypes', 't_stc_point', 'TAP 1.0 STC point',   'char', '*',   'adql:POINT',      1,0,0 ,31),
( 'tap_test.AllDataTypes', 't_stc_polygon', 'TAP 1.0 STC polygon', 'char', '*', 'adql:REGION',   1,0,0 ,32),

( 'tap_test.AllDataTypes', 't_dali_timestamp', 'DALI-1.1 timestamp', 'char', '*', 'timestamp',       1,0,0 ,40),
( 'tap_test.AllDataTypes', 't_dali_point', 'DALI-1.1 point',    'double', '2', 'point',         1,0,0 ,41),
( 'tap_test.AllDataTypes', 't_dali_circle', 'DALI-1.1 circle',  'double', '3', 'circle',        1,0,0 ,42),
( 'tap_test.AllDataTypes', 't_dali_polygon', 'DALI-1.1 polygon','double', '*', 'polygon',       1,0,0 ,43)
;

-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
update tap_schema.columns11 SET "size" = replace(arraysize::varchar,'*','')::int 
WHERE table_name LIKE 'tap_test.%'
  AND arraysize IS NOT NULL
  AND arraysize NOT LIKE '%x%'
  AND arraysize != '*';

