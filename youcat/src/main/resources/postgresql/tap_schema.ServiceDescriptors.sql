
create table tap_schema.ServiceDescriptors
(
    name varchar(128) not null primary key,
    value text not null,
    lastModified timestamp not null
)
;

-- grant select on all tables in schema tap_schema to public;