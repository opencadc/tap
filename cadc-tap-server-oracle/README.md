# cadc-tap-server-oracle 1.2.4

Offers Oracle support to handle datatype conversions in ADQL.

ALMA currently has a [sample implementation](https://github.com/opencadc/alma/tree/master/obscore).

## Requirements

This has only been tested with Oracle 11 _g_ XE and 12.

In order to use the ADQL `CONTAINS` and `INTERSECTS` functions, it is necessary to have `SDO_GEOMETRY` columns that 
contain the shape data, so that Oracle can create a Spatial Index on it.


