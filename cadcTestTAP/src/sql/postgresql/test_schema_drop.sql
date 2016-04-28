
-- drop all TEST tables in the correct order to not violate
-- foreign key constraints

drop table TESt_SCHEMA.key_columns;
drop table TEST_SCHEMA.keys;
drop table TEST_SCHEMA.columns;
drop table TEST_SCHEMA.tables;
drop table TEST_SCHEMA.schemas;
