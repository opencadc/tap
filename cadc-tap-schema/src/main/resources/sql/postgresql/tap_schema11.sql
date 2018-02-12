
-- minimal tap_schema creation

-- assumes that the tap_schema schema exists

-- sizes for fields are rather arbitrary and generous

-- tested with: PostgreSQL 8.x, 9.x
-- tested Sybase 15.x

create table tap_schema.schemas11
(
	schema_name   varchar(64)  NOT NULL,
	utype         varchar(512),
	description   varchar(512),
        schema_index  integer,	
	primary key (schema_name)
)
;


create table tap_schema.tables11
(
	schema_name   varchar(64)  NOT NULL,
	table_name    varchar(128) NOT NULL,
        table_type    varchar(8)   NOT NULL,
	utype         varchar(512),
	description   varchar(512),
	table_index   integer,
	
	primary key (table_name),
	foreign key (schema_name) references tap_schema.schemas11 (schema_name)
)
;

create table tap_schema.columns11
(
	table_name    varchar(128) NOT NULL,
	column_name   varchar(64)  NOT NULL,
	utype         varchar(512),
	ucd           varchar(64),
	unit          varchar(64),
	description   varchar(512),
	datatype      varchar(64)  NOT NULL,
-- TAP-1.1 arraysize
	arraysize     varchar(16),
-- TAP-1.1 xtype
        xtype         varchar(64),
-- TAP-1.1 size is deprecated
	"size"          integer,
	principal     integer      NOT NULL,
	indexed       integer      NOT NULL,
	std           integer      NOT NULL,
-- TAP-1.1 column_index
	column_index   integer,
-- extension: globally unique columnID for use as an XML ID attribute on the FIELD in VOTable output
        id            varchar(32),
	
	primary key (table_name,column_name),
	foreign key (table_name) references tap_schema.tables11 (table_name)
)
;


create table tap_schema.keys11
(
	key_id        varchar(64)  NOT NULL,
	from_table    varchar(128) NOT NULL,
	target_table  varchar(128) NOT NULL,
	utype         varchar(512),
	description   varchar(512),

	primary key (key_id),
	foreign key (from_table) references tap_schema.tables11 (table_name),
	foreign key (target_table) references tap_schema.tables11 (table_name)
)
;

create table tap_schema.key_columns11
(
	key_id          varchar(64) NOT NULL,
	from_column     varchar(64) NOT NULL,
	target_column   varchar(64) NOT NULL,

	foreign key (key_id) references tap_schema.keys11 (key_id)
)
;


