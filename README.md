# tap
client and server implementation of IVOA Table Access Protocol (TAP) specification

## youcat
YouCat is a complete prototype TAP service that supports TAP-1.1 plus TAP-next extensions
for user-managed tables. Users are allocated a schema (literally) and can create and drop tables, 
create indices, and load table data. Loading data is limited to "append more rows" and designed
mainly for bulk data loading. 

## cadc-*
These are libraries used to build `youcat` and other services and applications.

- cadc-adql: ADQL parser
- cadc-tap: TAP data model and common client/server code
- cadc-tap-schema: server side tap_schema implementation
- cadc-tap-server: server side TAP server support
- cadc-tap-server-oracle: Oracle specific server customizations
- cadc-tap-server-pg: PostgreSQL specific server customizations
- cadc-tap-tmp: temporary file storage to support tap_upload and async result storage
- cadc-test-tap: service test suite
- cadc-jsqlparser-compat: a fork of an old version used by `cadc-adql`

## example-tap - OBSOLETE
