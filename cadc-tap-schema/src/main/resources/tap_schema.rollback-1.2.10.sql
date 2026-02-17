alter table tap_schema.tables11 drop column view_target;

update tap_schema.ModelVersion 
set version='1.2.5' where model='TAP_SCHEMA';

