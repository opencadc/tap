
-- minimal tap_schema creation

-- assumes that the tap_schema schema exists

-- sizes for fields are rather arbitrary and generous

-- tested with: PostgreSQL 9.x, 10.x

create table tap_schema.schemas11
(
	schema_name   varchar(64)  NOT NULL,
	utype         varchar(512),
	description   varchar(512),
        schema_index  integer,
	
-- extension: permissions for user-created content
    owner_id        varchar(256),
    read_anon       integer,
    read_only_group  varchar(128),
    read_write_group varchar(128),

-- extension: flag to indicate if a schema was created using a Tap service API
    api_created     integer,

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

-- extension: permissions for user-created content
    owner_id        varchar(256),
    read_anon       integer,
    read_only_group  varchar(128),
    read_write_group varchar(128),

-- extension: flag to indicate if a table was created using a Tap service API
    api_created     integer,

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
    column_id     varchar(32),
	
-- extension: permissions for user-created content
    owner_id        varchar(256),
    read_anon       integer,
    read_only_group  varchar(128),
    read_write_group varchar(128),

	primary key (table_name,column_name),
	foreign key (table_name) references tap_schema.tables11 (table_name)
)
;

create unique index columns_column_id
    on tap_schema.columns11 (column_id)
    where column_id is not null
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


