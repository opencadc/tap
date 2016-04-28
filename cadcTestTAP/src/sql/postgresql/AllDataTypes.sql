
-- a sample table with every PostgreSQL datatype in current use
-- the table has one row with valid values for each type

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


grant select on table TEST.AllDataTypes to CVOPUB;


