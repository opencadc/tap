
create table tap_schema.ServiceDescriptorTemplate
(
    name varchar(128) not null primary key,
    value text not null,
    lastModified timestamp not null
)
;
