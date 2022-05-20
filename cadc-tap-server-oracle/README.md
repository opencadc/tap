# cadc-tap-server-oracle 1.2.9

Offers Oracle support to handle datatype conversions in ADQL.

ALMA currently has a [sample implementation](https://github.com/opencadc/alma/tree/master/tap).

## Requirements

This has been tested with Oracle 11 _g_ XE, 12, and 19.

In order to use the ADQL `CONTAINS` and `INTERSECTS` functions, it is necessary to have `SDO_GEOMETRY` columns that 
contain the shape data, so that Oracle can create a Spatial Index on it.  It is necessary for your database to have
the proper `SRID` initialized for all geometric objects.  See [Oracle Spatial Concepts](https://docs.oracle.com/en/database/oracle/oracle-database/19/spatl/spatial-concepts.html).
