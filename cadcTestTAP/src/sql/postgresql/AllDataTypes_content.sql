
-- a sample table with currently support PostgreSQL datatype 
-- the table has one row with valid values for each type
-- the table has one row with NULL values for each type

drop table TEST.AllDataTypes;

create table TEST.AllDataTypes
(
    t_integer integer,
    t_long    bigint,
    t_double  double precision,
    t_string  varchar(64),
    t_date    timestamp,
    t_point   spoint,
    t_circle  scircle,
    t_poly    spoly
)
tablespace test_tap_data
;


insert into TEST.AllDataTypes 
(
t_integer, t_long, t_double, t_string, t_date,
t_point,
t_circle,
t_poly
) 
values
( 
1, 1, 1.0, 
-- string aka varchar
'hello', 
-- date aka timestamp in IVOA ISO8601 format
'2009-01-02T03:04:05.678', 
-- point
point '(10.0d, 10.0d)',
-- circle
circle '<(10.0d, 10.0d), 1.0d>',
-- poly
poly '{ (2.0d, 2.0d), (2.0d, 4.0d), (3.0d, 3.0d) }'

)
;

-- cleanup TEST description
delete from TEST.columns where table_name = 'TEST.AllDataTypes';
delete from TEST.tables where table_name = 'TEST.AllDataTypes';

insert into TEST.tables (schema_name,table_name,description) values
( 'TEST', 'TEST.AllDataTypes', 'sample table with all current internal datatypes and one row of values');

insert into TEST.columns (table_name,column_name,description,ucd,unit,datatype,size,principal,indexed,std) values
( 'TEST.AllDataTypes', 't_integer', 'integer (32-bit)',  NULL, NULL, 'adql:INTEGER', NULL, 1,0,0 ),
( 'TEST.AllDataTypes', 't_long', 'long (64-bit)',  NULL, NULL, 'adql:BIGINT', NULL, 1,0,0 ),
( 'TEST.AllDataTypes', 't_double', 'double (64-bit)',  NULL, NULL, 'adql:DOUBLE', NULL, 1,0,0 ),
( 'TEST.AllDataTypes', 't_string', 'string',  NULL, NULL, 'adql:VARCHAR', 64, 1,0,0 ),
( 'TEST.AllDataTypes', 't_date', 'Date', NULL, NULL, 'adql:TIMESTAMP', NULL, 1,0,0 ),
( 'TEST.AllDataTypes', 't_spoint', 'PgSphere spoint', NULL, NULL, 'adql:POINT', NULL, 1,0,0 ),
( 'TEST.AllDataTypes', 't_scircle', 'PgSphere scircle', NULL, NULL, 'adql:REGION', NULL, 1,0,0 ),
( 'TEST.AllDataTypes', 't_spoly', 'PgSphere spoly', NULL, NULL, 'adql:REGION', NULL, 1,0,0 )
;

grant select on table TEST.AllDataTypes to CVOPUB;


