# cadc-tap-server-oracle 1.2.2

Offers Oracle support to handle datatype conversions in ADQL.

ALMA currently has a [sample implementation](https://github.com/opencadc/alma-tap).

## Requirements

This has only been tested with Oracle 11 _g_ XE.

In order to use the ADQL `CONTAINS` and `INTERSECTS` functions, it is necessary to have `SDO_GEOMETRY` columns that 
contain the shape data, so that Oracle can create a Spatial Index on it.


