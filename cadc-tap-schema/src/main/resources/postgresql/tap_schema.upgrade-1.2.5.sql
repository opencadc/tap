--
-- upgrade from 1.2.1 to 1.2.5

alter table tap_schema.columns11 rename column id to column_id;
create unique index column_id_unique_not_null on tap_schema.columns11 (column_id) where column_id is not null;
