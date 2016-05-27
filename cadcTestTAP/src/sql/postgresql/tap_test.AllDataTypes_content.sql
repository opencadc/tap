
-- a sample table with currently support PostgreSQL datatype 
-- the table has one row with valid values for each type
-- the table has one row with NULL values for each type

drop table tap_test.AllDataTypes;

create table tap_test.AllDataTypes
(
    t_char                character,
    t_clob                character varying,
    t_char_n              character(8),
    t_string              character varying,
    t_string_n            character varying(64),
    t_unsignedByte_1      bytea,
    t_blob                bytea,
    t_unsignedByte_n      bytea(16),
    t_unsignedByte_n_any  bytea[],
    t_short               smallint,
    t_int                 integer,
    t_long                bigint,
    t_float               real,
    t_double              double precision,
    t_date                timestamp,
    t_poly_char           spoly,
    t_point_char          spoint
)
tablespace tap_test_tap_data
;


insert into tap_test.AllDataTypes 
(
    t_char,
    t_clob,
    t_char_n, 
    t_string,
    t_string_n,
    t_unsignedByte_1, 
    t_blob,          
    t_unsignedByte_n,
    t_unsignedByte_n_any,
    t_short,            
    t_int,             
    t_long,           
    t_float,         
    t_double,       
    t_date,        
    t_poly_char,  
    t_point_char
) 
values
( 
-- single character
'h',
-- CLOB
'test data for character large object, CLOB',
-- string with fixed length
'fixedlen',
-- string with variable length
'variable length character string',
-- string with variable length with length set to 64 max.
'variable length character string with length set to 64 max.',
-- single quote in binary
'''',
-- BLOB
'\\000\\000\\000\\004@B1\\224\\033\\274n\\304\\300\\022\\023m\\002Nk\\220\\000\\000\\000\\002@B1\\227\\274\\033n!\\300\\022\\304\\301\\350\\026n0\\000\\000\\000\\001@B\\033W\\024\\256\\2362\\300\\022\\304\\327\\364=\\331\\360\\000\\000\\000\\001@B\\033T\\342;M\\357\\300\\022\\023\\203D\\022V0\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000@BIAD\\215]\\332\\300\\022\\021\\350\\325\\303\\216P\\000\\000\\000\\002@BIZ\\235cV\\261\\300\\022\\303987H\\220\\000\\000\\000\\001@B3\\036Il\\036\\377\\300\\022\\303\\374\\327\\276n\\020\\000\\000\\000\\001@B3\\006]\\227\\300b\\300\\022\\022\\254.gw`\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000@B1\\215\\201a\\316\\275\\300\\022\\321\\244w\\035\\225 \\000\\000\\000\\002@B1\\2324\\372\\273\\026\\300\\023\\202\\3528\\314\\312p\\000\\000\\000\\001@B\\033c;2\\236\\224\\300\\023\\203I@\\262\\365\\360\\000\\000\\000\\001@B\\033W\\367\\244o\\332\\300\\022\\322\\003\\256\\275{`\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000@BIkK\\012\\320\\011\\300\\022\\320/\\322_\\247\\020\\000\\000\\000\\002@BI{\\322\\026\\011\\376\\300\\023\\201\\235\\233\\207n\\260\\000\\000\\000\\001@B3:\\344\\375b\\027\\300\\023\\202\\031X\\207>\\020\\000\\000\\000\\001@B3+\\312\\3420\\014\\300\\022\\320\\253Mv6\\020\\000\\000\\000\\001\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\000\\001'
-- binary with fixed length
'\\000\\000\\000\\001@T\\363\\325\\231$(\\324\\300\\025\\200\\253\\3641\\215\\015\\001'
-- array of binary
'{{\\141\\162\\162\\141\\171},{\\157\\146},{\\142\\151\\156\\141\\162\\171}}',
-- short
1
-- int
2
-- long
9223372036854775807
-- float
0.299955,
-- double
0.299999999999955,
-- date aka timestamp in IVOA ISO8601 format
'2009-01-02T03:04:05.678', 
-- poly
polygon('ICRS',1,4,2,4,2,5,1,5),
-- point
Point('GALACTIC', 10, 20)

)
;


insert into tap_test.AllDataTypes 
(
    t_char,
    t_clob,
    t_char_n, 
    t_string,
    t_string_64,
    t_unsignedByte_1, 
    t_blob,          
    t_unsignedByte_n,
    t_unsignedByte_n_any,
    t_short,            
    t_int,             
    t_long,           
    t_float,         
    t_double,       
    t_date,        
    t_poly_char,  
    t_point_char
) 
values
( 
-- a row of NULL values
NULL,
-- CLOB
NULL,
-- string with fixed length
NULL,
-- string with variable length
NULL,
-- string with variable length with length set to 64 max.
NULL,
-- single quote in binary
NULL,
-- BLOB
NULL,
-- binary with fixed length
NULL,
-- array of binary
NULL,
-- short
NULL,
-- int
NULL,
-- long
NULL,
-- float
NULL,
-- double
NULL,
-- date aka timestamp in IVOA ISO8601 format
NULL,
-- poly
NULL,
-- point
NULL
)
;
