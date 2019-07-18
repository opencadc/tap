--
-- upgrade from 1.1.6 to 1.2.0
-- add permission columns to support user-created content


alter table tap_schema.schemas11
    add column owner_id         varchar(32),
    add column read_anon        integer,
    add column read_only_group  varchar(128),
    add column read_write_group varchar(128);

alter table tap_schema.tables11
    add column owner_id         varchar(32),
    add column read_anon        integer,
    add column read_only_group  varchar(128),
    add column read_write_group varchar(128);
