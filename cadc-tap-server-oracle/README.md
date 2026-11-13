# cadc-tap-server-oracle 1.2.9

Offers Oracle support to handle datatype conversions in ADQL.

ALMA currently has a [sample implementation](https://github.com/opencadc/alma/tree/master/tap).

## Requirements

This has been tested with Oracle 11 _g_ XE, 12, and 19.

For TAP **UPLOAD**, use `ca.nrc.cadc.tap.upload.OracleUploadManager` instead of
`BasicUploadManager`. It loads rows through `OracleTableLoader`, which binds DALI `point` /
`polygon` cells as `MDSYS.SDO_GEOMETRY` (generic `TableLoader` leaves them as DALI objects and
Oracle JDBC reports *invalid column type*, typically on `position_bounds` first).

Other upload xtypes (`uuid`, `uri`, `interval`, `timestamp`) use `OracleDataType` for DDL and
`TableLoader` value conversion; no extra loader logic is required for those columns.

In order to use the ADQL `CONTAINS` and `INTERSECTS` functions, it is necessary to have `SDO_GEOMETRY` columns that 
contain the shape data, so that Oracle can create a Spatial Index on it.  It is necessary for your database to have
the proper `SRID` initialized for all geometric objects.  See [Oracle Spatial Concepts](https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/spatial-concepts.html).
