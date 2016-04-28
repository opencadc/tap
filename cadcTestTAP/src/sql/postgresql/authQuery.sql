
-- a table with restricted access to be used by authenticated query tests.

-- cleanup TEST description
delete from TEST.columns where table_name = 'TEST.authQuery';
delete from TEST.tables where table_name = 'TEST.authQuery';

insert into TEST.tables (schema_name,table_name,description,metaReadAccessGroups) values
( 'TEST', 'TEST.authQuery', 'table with restricted access to be used authenticated queries', 'CADC-TEST');

insert into TEST.columns (table_name,column_name,description,size,principal,indexed,std,description,unit,columnID,metaReadAccessGroups) values
( 'TEST.authQuery', 'NUMBER', 'adql:INTEGER', NULL, 0, 0, 0, 'An integer column', NULL, 1000, 'CADC-TEST')
;
