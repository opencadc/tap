--
-- upgrade from 1.2.0 to 1.2.1
-- add api_created to flag a table or schema was created using the TAP API

alter table tap_schema.tables11 add column api_created integer;
alter table tap_schema.schemas11 add column api_created integer;
