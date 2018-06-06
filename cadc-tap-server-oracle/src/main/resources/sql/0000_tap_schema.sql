-- minimal TAP_SCHEMA creation
-- assumes that the TAP_SCHEMA schema exists
-- sizes for fields are rather arbitrary and generous
-- tested with: PostgreSQL 8.x, 9.x
-- tested Sybase 15.x
CREATE TABLE TAP_SCHEMA.schemas (
  schema_name varchar(64) NOT NULL,
  utype varchar(512),
  description varchar(512),
  PRIMARY KEY (schema_name)
);

GRANT SELECT ON TAP_SCHEMA.schemas TO public;

CREATE TABLE TAP_SCHEMA.tables (
  schema_name varchar(64) NOT NULL,
  table_name varchar(128) NOT NULL,
  table_type varchar(8) NOT NULL,
  utype varchar(512),
  description varchar(512),
  table_index integer,
  PRIMARY KEY (table_name),
  FOREIGN KEY (schema_name) REFERENCES TAP_SCHEMA.schemas (schema_name)
);

GRANT SELECT ON TAP_SCHEMA.tables TO public;

CREATE TABLE TAP_SCHEMA.columns (
  table_name varchar(128) NOT NULL,
  column_name varchar(64) NOT NULL,
  utype varchar(512),
  ucd varchar(64),
  unit varchar(64),
  description varchar(512),
  datatype varchar(64) NOT NULL, -- TAP-1.1 arraysize
  arraysize integer, -- TAP-1.1 size is deprecated
  "size" integer,
  principal integer NOT NULL,
  indexed integer NOT NULL,
  std integer NOT NULL, -- TAP-1.1 column_index
  column_index integer, -- extension: globally unique columnID for use as an XML ID attribute on the FIELD in VOTable output
  id varchar(32),
  PRIMARY KEY (table_name, column_name),
  FOREIGN KEY (table_name) REFERENCES TAP_SCHEMA.tables (table_name)
);

GRANT SELECT ON TAP_SCHEMA.columns TO public;

CREATE TABLE TAP_SCHEMA.keys (
  key_id varchar(64) NOT NULL,
  from_table varchar(128) NOT NULL,
  target_table varchar(128) NOT NULL,
  utype varchar(512),
  description varchar(512),
  PRIMARY KEY (key_id),
  FOREIGN KEY (from_table) REFERENCES TAP_SCHEMA.tables (table_name),
  FOREIGN KEY (target_table) REFERENCES TAP_SCHEMA.tables (table_name)
);

GRANT SELECT ON TAP_SCHEMA.keys TO public;

CREATE TABLE TAP_SCHEMA.key_columns (
  key_id varchar(64) NOT NULL,
  from_column varchar(64) NOT NULL,
  target_column varchar(64) NOT NULL,
  FOREIGN KEY (key_id) REFERENCES TAP_SCHEMA.keys (key_id)
);

GRANT SELECT ON TAP_SCHEMA.key_columns TO public;
