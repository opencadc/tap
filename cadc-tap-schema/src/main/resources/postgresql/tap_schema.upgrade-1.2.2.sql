--
-- upgrade from 1.2.1 to 1.2.2
-- add api_created to flag a table or schema was created using the TAP API
-- update tap_schema.columns11 id column to column_id

alter table tap_schema.columns11 rename column id to column_id;
