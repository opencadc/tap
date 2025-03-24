--
-- upgrade from 1.2.1 to 1.2.5

alter table tap_schema.columns11 rename column id to column_id;
create unique index columns_column_id on tap_schema.columns11 (column_id) where column_id is not null;

alter table tap_schema.schemas11 alter column owner_id set data type varchar(256);
alter table tap_schema.tables11 alter column owner_id set data type varchar(256);
alter table tap_schema.columns11 alter column owner_id set data type varchar(256);