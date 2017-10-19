
-- minimal TAP_SCHEMA creation

-- sizes for fields are rather arbitrary and generous
-- tested Sybase 15.x

create table tap_schema_schemas
(
	schema_name   varchar(64)  NOT NULL,
	utype         varchar(512) NULL,
	description   varchar(512) NULL,
	
	primary key (schema_name)
)
;


create table tap_schema_tables
(
	schema_name   varchar(64)  NOT NULL,
	table_name    varchar(128) NOT NULL,
        table_type    varchar(8)   NOT NULL,
	utype         varchar(512) NULL,
	description   varchar(512) NULL,
-- TAP-1.1 table_index
        table_index   integer      NULL,
	
	primary key (table_name),
	foreign key (schema_name) references tap_schema_schemas (schema_name)
)
;

create table tap_schema_columns
(
	table_name    varchar(128) NOT NULL,
	column_name   varchar(64)  NOT NULL,
	utype         varchar(512) NULL,
	ucd           varchar(64)  NULL,
	unit          varchar(64)  NULL,
	description   varchar(512) NULL,
	datatype      varchar(64)  NOT NULL,
-- TAP-1.1 arraysize
	arraysize     integer      NULL,
-- TAP-1.1 size is deprecated
	size          integer      NULL,
	principal     integer      NOT NULL,
	indexed       integer      NOT NULL,
	std           integer      NOT NULL,
-- TAP-1.1 column_index
	column_index  integer      NULL,

-- globally unique columnID for use as an XML ID attribute on the FIELD in VOTable output
        id            varchar(32)  NULL,
	
	primary key (table_name,column_name),
	foreign key (table_name) references tap_schema_tables (table_name)
)
;


create table tap_schema_keys
(
	key_id        varchar(64)  NOT NULL,
	from_table    varchar(128) NOT NULL,
	target_table  varchar(128) NOT NULL,
	utype         varchar(512) NULL,
	description   varchar(512) NULL,

	primary key (key_id),
	foreign key (from_table) references tap_schema_tables (table_name),
	foreign key (target_table) references tap_schema_tables (table_name)
)
;

create table tap_schema_key_columns
(
	key_id          varchar(64) NOT NULL,
	from_column     varchar(64) NOT NULL,
	target_column   varchar(64) NOT NULL,

	foreign key (key_id) references tap_schema_keys (key_id)
)
;


