-- content of the TAP_SCHEMA tables that describes the TAP_SCHEMA itself
-- the 11 suffix on all physical table names means this is the TAP-1.1 version
-- delete key columns for keys from tables in the TAP_SCHEMA schema
DELETE FROM TAP_SCHEMA.key_columns11
WHERE key_id IN (
    SELECT
      key_id FROM TAP_SCHEMA.keys11
      WHERE from_table IN (
          SELECT
            table_name FROM TAP_SCHEMA.tables11
            WHERE lower(table_name)
              LIKE 'TAP_SCHEMA.%')
            OR target_table IN (
              SELECT
                table_name FROM TAP_SCHEMA.tables11
                WHERE lower(table_name)
                  LIKE 'TAP_SCHEMA.%'));

-- delete keys from tables in the TAP_SCHEMA schema
DELETE FROM TAP_SCHEMA.keys11
WHERE from_table IN (
    SELECT
      table_name FROM TAP_SCHEMA.tables11
      WHERE lower(table_name)
        LIKE 'TAP_SCHEMA.%')
      OR target_table IN (
        SELECT
          table_name FROM TAP_SCHEMA.tables11
          WHERE lower(table_name)
            LIKE 'TAP_SCHEMA.%');

-- delete columns from tables in the TAP_SCHEMA schema
DELETE FROM TAP_SCHEMA.columns11
WHERE table_name IN (
    SELECT
      table_name FROM TAP_SCHEMA.tables11
      WHERE lower(table_name)
        LIKE 'TAP_SCHEMA.%');

-- delete tables
DELETE FROM TAP_SCHEMA.tables11
WHERE lower(table_name)
  LIKE 'TAP_SCHEMA.%';

-- delete schema
DELETE FROM TAP_SCHEMA.schemas11
WHERE lower(schema_name) = 'TAP_SCHEMA';

INSERT INTO TAP_SCHEMA.schemas11 (schema_name, description, utype)
  VALUES ('TAP_SCHEMA', 'a special schema to describe TAP-1.1 tablesets', NULL);

INSERT ALL
  INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('TAP_SCHEMA', 'TAP_SCHEMA.schemas', 'table', 'description of schemas in this tableset', NULL, 1)
  INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES('TAP_SCHEMA', 'TAP_SCHEMA.tables', 'table', 'description of tables in this tableset', NULL, 2)
  INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES('TAP_SCHEMA', 'TAP_SCHEMA.columns', 'table', 'description of columns in this tableset', NULL, 3)
  INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES('TAP_SCHEMA', 'TAP_SCHEMA.keys', 'table', 'description of foreign keys in this tableset', NULL, 4)
  INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES('TAP_SCHEMA', 'TAP_SCHEMA.key_columns', 'table', 'description of foreign key columns in this tableset', NULL, 5)
SELECT 1 FROM DUAL;

INSERT ALL
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.schemas', 'schema_name', 'schema name for reference to TAP_SCHEMA.schemas', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 1)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.schemas', 'utype', 'lists the utypes of schemas in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 2)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.schemas', 'description', 'describes schemas in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 3)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.tables', 'schema_name', 'the schema this table belongs to', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 1)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.tables', 'table_name', 'the fully qualified table name', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 2)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.tables', 'table_type', 'one of: table view', NULL, NULL, NULL, 'char', '8*', NULL, 1, 0, 0, 3)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.tables', 'utype', 'lists the utype of tables in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 4)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.tables', 'description', 'describes tables in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 5)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.tables', 'table_index', 'recommended sort order when listing tables', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 6)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'table_name', 'the table this column belongs to', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 1)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'column_name', 'the column name', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 2)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'utype', 'lists the utypes of columns in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 3)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'ucd', 'lists the UCDs of columns in the tableset', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 4)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'unit', 'lists the unit used for column values in the tableset', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 5)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'description', 'describes the columns in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 6)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 7)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'arraysize', 'lists the size of variable-length columns in the tableset', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 8)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'xtype', 'a DALI or custom extended type annotation', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 7)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', '"size"', 'deprecated: use arraysize', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 9)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'principal', 'a principal column; 1 means 1, 0 means 0', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 10)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'indexed', 'an indexed column; 1 means 1, 0 means 0', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 11)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'std', 'a standard column; 1 means 1, 0 means 0', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 12)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.columns', 'column_index', 'recommended sort order when listing columns of a table', NULL, NULL, NULL, 'int', NULL, NULL, 1, 0, 0, 13)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.keys', 'key_id', 'unique key to join to TAP_SCHEMA.key_columns', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 1)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.keys', 'from_table', 'the table with the foreign key', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 2)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.keys', 'target_table', 'the table with the primary key', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 3)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.keys', 'utype', 'lists the utype of keys in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 4)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.keys', 'description', 'describes keys in the tableset', NULL, NULL, NULL, 'char', '512*', NULL, 1, 0, 0, 5)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.key_columns', 'key_id', 'key to join to TAP_SCHEMA.keys', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 1)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.key_columns', 'from_column', 'column in the from_table', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 2)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, description, utype, ucd, unit, datatype, arraysize, xtype, principal, indexed, std, column_index)
  VALUES ('TAP_SCHEMA.key_columns', 'target_column', 'column in the target_table', NULL, NULL, NULL, 'char', '64*', NULL, 1, 0, 0, 3)
SELECT 1 FROM DUAL;

INSERT ALL
  INTO TAP_SCHEMA.keys11 (key_id, from_table, target_table)
  VALUES ('k1', 'TAP_SCHEMA.tables', 'TAP_SCHEMA.schemas')
  INTO TAP_SCHEMA.keys11 (key_id, from_table, target_table)
  VALUES ('k2', 'TAP_SCHEMA.columns', 'TAP_SCHEMA.tables')
  INTO TAP_SCHEMA.keys11 (key_id, from_table, target_table)
  VALUES ('k3', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables')
  -- two separate foreign keys: see below
  INTO TAP_SCHEMA.keys11 (key_id, from_table, target_table)
  VALUES ('k4', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables')
  -- two separate foreign keys: see below
  INTO TAP_SCHEMA.keys11 (key_id, from_table, target_table)
  VALUES ('k5', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.keys')
SELECT 1 FROM DUAL;  

INSERT ALL 
  INTO TAP_SCHEMA.key_columns11 (key_id, from_column, target_column)
  VALUES ('k1', 'schema_name', 'schema_name')
  INTO TAP_SCHEMA.key_columns11 (key_id, from_column, target_column)
  VALUES ('k2', 'table_name', 'table_name')
  INTO TAP_SCHEMA.key_columns11 (key_id, from_column, target_column)
  VALUES ('k3', 'from_table', 'table_name')
  INTO TAP_SCHEMA.key_columns11 (key_id, from_column, target_column)
  VALUES ('k4', 'target_table', 'table_name')
  INTO TAP_SCHEMA.key_columns11 (key_id, from_column, target_column)
  VALUES ('k5', 'key_id', 'key_id')
SELECT 1 FROM DUAL;

-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
UPDATE
  TAP_SCHEMA.columns11
SET
  "size" = TO_NUMBER(REPLACE(arraysize, '*', ''))
WHERE
  table_name LIKE 'TAP_SCHEMA.%'
AND arraysize IS NOT NULL
AND arraysize NOT LIKE '%x%';
