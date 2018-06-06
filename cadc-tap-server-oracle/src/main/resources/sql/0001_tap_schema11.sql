-- minimal tap_schema creation
-- assumes that the tap_schema schema exists
-- sizes for fields are rather arbitrary and generous
-- tested with: Oracle 11g XE
CREATE TABLE TAP_SCHEMA.schemas11 (
  schema_name varchar(64) NOT NULL,
  utype varchar(512),
  description varchar(512),
  schema_index integer,
  PRIMARY KEY (schema_name)
);

GRANT SELECT ON TAP_SCHEMA.schemas11 TO public;

CREATE TABLE TAP_SCHEMA.tables11 (
  schema_name varchar(64) NOT NULL,
  table_name varchar(128) NOT NULL,
  table_type varchar(8) NOT NULL,
  utype varchar(512),
  description varchar(512),
  table_index integer,
  PRIMARY KEY (table_name),
  FOREIGN KEY (schema_name) REFERENCES tap_schema.schemas11 (schema_name)
);

GRANT SELECT ON TAP_SCHEMA.tables11 TO public;

CREATE TABLE TAP_SCHEMA.columns11 (
  table_name varchar(128) NOT NULL,
  column_name varchar(64) NOT NULL,
  utype varchar(512),
  ucd varchar(64),
  unit varchar(64),
  description varchar(512),
  datatype varchar(64) NOT NULL, -- TAP-1.1 arraysize
  arraysize varchar(16), -- TAP-1.1 xtype
  xtype varchar(64), -- TAP-1.1 size is deprecated
  "size" integer,
  principal integer NOT NULL,
  indexed integer NOT NULL,
  std integer NOT NULL, -- TAP-1.1 column_index
  column_index integer, -- extension: globally unique columnID for use as an XML ID attribute on the FIELD in VOTable output
  id varchar(32),
  PRIMARY KEY (table_name, column_name),
  FOREIGN KEY (table_name) REFERENCES tap_schema.tables11 (table_name)
);

GRANT SELECT ON TAP_SCHEMA.columns11 TO public;

CREATE TABLE TAP_SCHEMA.keys11 (
  key_id varchar(64) NOT NULL,
  from_table varchar(128) NOT NULL,
  target_table varchar(128) NOT NULL,
  utype varchar(512),
  description varchar(512),
  PRIMARY KEY (key_id),
  FOREIGN KEY (from_table) REFERENCES tap_schema.tables11 (table_name),
  FOREIGN KEY (target_table) REFERENCES tap_schema.tables11 (table_name)
);

GRANT SELECT ON TAP_SCHEMA.keys11 TO public;

CREATE TABLE TAP_SCHEMA.key_columns11 (
  key_id varchar(64) NOT NULL,
  from_column varchar(64) NOT NULL,
  target_column varchar(64) NOT NULL,
  FOREIGN KEY (key_id) REFERENCES tap_schema.keys11 (key_id)
);

GRANT SELECT ON TAP_SCHEMA.columns11 TO public;
