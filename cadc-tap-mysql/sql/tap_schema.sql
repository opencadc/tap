 #CREATE SCHEMA IF NOT EXISTS TAP_SCHEMA;

 # minimal TAP.creation
 # assumes that the TAP.schema exists
 # sizes for fields are rather arbitrary and generous
 # tested with: PostgreSQL 8.x, 9.x, 10.x
CREATE TABLE TAP.schemas (
  schema_name varchar(64) NOT NULL,
  utype varchar(512),
  description varchar(512),
  PRIMARY KEY (schema_name)
);

CREATE TABLE TAP.tables (
  schema_name varchar(64) NOT NULL,
  table_name varchar(128) NOT NULL,
  table_type varchar(8) NOT NULL,
  utype varchar(512),
  description varchar(512),
  table_index integer,
  PRIMARY KEY (table_name),
  FOREIGN KEY (schema_name) REFERENCES TAP.schemas (schema_name)
);

CREATE TABLE TAP.columns (
  table_name varchar(128) NOT NULL,
  column_name varchar(64) NOT NULL,
  utype varchar(512),
  ucd varchar(64),
  unit varchar(64),
  description varchar(512),
  datatype varchar(64) NOT NULL, -- TAP-1.1 arraysize
  arraysize integer, -- TAP-1.1 size is deprecated
  size integer,
  principal integer NOT NULL,
  indexed integer NOT NULL,
  std integer NOT NULL, -- TAP-1.1 column_index
  column_index integer, -- extension: globally unique columnID for use as an XML ID attribute on the FIELD in VOTable output
  id varchar(32),
  PRIMARY KEY (table_name,
    column_name),
  FOREIGN KEY (table_name) REFERENCES TAP.tables (table_name)
);

CREATE TABLE TAP.keys (
  key_id varchar(64) NOT NULL,
  from_table varchar(128) NOT NULL,
  target_table varchar(128) NOT NULL,
  utype varchar(512),
  description varchar(512),
  PRIMARY KEY (key_id),
  FOREIGN KEY (from_table) REFERENCES TAP.tables (table_name),
  FOREIGN KEY (target_table) REFERENCES TAP.tables (table_name)
);

CREATE TABLE TAP.key_columns (
  key_id varchar(64) NOT NULL,
  from_column varchar(64) NOT NULL,
  target_column varchar(64) NOT NULL,
  FOREIGN KEY (key_id) REFERENCES TAP.keys (key_id)
);

GRANT ALL PRIVILEGES ON *.* TO 'TAP_SCHEMA'@'%';
#GRANT ALL ON TAP.* TO 'TAP_SCHEMA'@'%';
# GRANT SELECT ON TAP_SCHEMA.* TO 'TAP_SCHEMA';
