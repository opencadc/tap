# cadc-tap-server-pg 1.0.0

Offers PostgreSQL support to handle datatype conversions and ADQL 
transformations for a PostgreSQL+pgsphere backend database.

The `ca.nrc.cadc.tap.pg.PostgresDataTypeMapper` extends the default `BasicDataTypeMapper`
with pg+pgsphere specific datatype support.

The `ca.nrc.cadc.tap.writer.format` package provides a FormatFactory and Format implementations
to support pg+pgsphere specific datatypes.

The `ca.nrc.cadc.tap.parser` package provides various extensions of the `cadc-adql` ADQL 
parser to support pg+pgsphere specific ADQL transformations.

## example usage
The `youcat` TAP service makes use of all the features of this library.
