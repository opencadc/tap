# table query service with user-managed tables (youcat)

This service is a complete TAP service implementation that supports the
IVOA <a href="http://www.ivoa.net/documents/TAP/20190927/">TAP-1.1</a> web 
service API plus prototype WD-TAP-1.2 extensions to enable users to create 
their own tables and load data into them.

The extended features and current implementation are aimed primarily at 
creating and bulk-loading of astronomical catalogues, but it can be used 
as a generic TAP implementation(see configuration limitations, development
plans, and enahncements needed below).

## backend database support
The current implementation is hard-coded to work with postgresql and the pgsphere
extension for spherical geometry support (see development plans below).

The minimum setup is to have two database accounts, logically:
- `tapadm` to create and manage all the tables
- `tapuser` for executing queries that are submitted using the TAP API

The following schemas are required:
- tap_schema (`tapadm` user must have authorization to create objects)
- uws (`tapadm` user must have authorization to create objects)
- tap_upload (`tapuser` user must have authorization to create tables)

See _createSchemaInDB_ in the youcat.properties config below for additional optional
database permission detail.

The REST API supports a mechanism to _ingest_ an existing table into the tap_schema.
An additional account to manage content (directly connect to the database to create and
populate tables) could/should be used for this.

## configuration

The following configuration files must be available in the `/config` directory.

### catalina.properties

This file contains java system properties to configure the tomcat server and some
of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties. 

`youcat` includes multiple IdentityManager implementations to support authenticated access:
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
  
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.
 
`youcat` requires 3 connection pools:
```
org.opencadc.youcat.tapadm.maxActive={max connections for jobs pool}
org.opencadc.youcat.tapadm.username={database username for jobs pool}
org.opencadc.youcat.tapadm.password={database password for jobs pool}
org.opencadc.youcat.tapadm.url=jdbc:postgresql://{server}/{database}

org.opencadc.youcat.tapuser.maxActive={max connections for jobs pool}
org.opencadc.youcat.tapuser.username={database username for jobs pool}
org.opencadc.youcat.tapuser.password={database password for jobs pool}
org.opencadc.youcat.tapuser.url=jdbc:postgresql://{server}/{database}

org.opencadc.youcat.uws.maxActive={max connections for jobs pool}
org.opencadc.youcat.uws.username={database username for jobs pool}
org.opencadc.youcat.uws.password={database password for jobs pool}
org.opencadc.youcat.uws.url=jdbc:postgresql://{server}/{database}
```

The `tapadm` pool manages (create, alter, drop) tap_schema tables and manages the tap_schema content. The `uws` 
pool manages (create, alter, drop) uws tables and manages the uws content (creates and modifies jobs in the uws
schema when jobs are created and executed by users. If `youcat` is configured with to create schemas (server _createSchemaInDB_ below) then this pool must also have permission to create schemas.

The `tapuser` pool is used to run TAP queries, including creating tables in the `tap_upload` schema. 

All three pools must have the same JDBC URL (e.g. use the same database) with PostgreSQL. This may be 
relaxed in future.

### cadc-registry.properties

The `youcat` service currently queries a known (trusted) GMS service for all groups a user belongs to
in order to inject group membership constraints into queries on the *tap_schema* content. A service 
providing the `ivo://ivoa.net/std/GMS#search-1.0` capability must be configured here for that to work. 
Temporary hack: at least one library still uses the old standardID (`ivo://ivoa.net/std/GMS#search-0.1`) so that 
has to be configured as well.

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### cadc-tap-tmp.properties

This service uses the `cadc-tap-tmp` library for temporary storage needed by core TAP features 
(async results, tap_upload tables). It is currently hard-coded (PluginFactory.properties) to 
use the DelegatingStorageManager.

This storage _is not_ used for content upload of user-created tables: that content is streamed 
directly into the database server.

See <a href="https://github.com/opencadc/tap/tree/master/cadc-tap-tmp">cadc-tap-tmp</a>.

### youcat.properties

The youcat.properties configures some admin and optional functions of the service.
```
# (optional) configure the admin user
org.opencadc.youcat.adminUser = {identity}

# (optional) schema creation in the database (default: false)
org.opencadc.youcat.createSchemaInDB = true|false
```
The optional _adminUser_ (configured using the network username) can use the youcat API to create a 
new schema for a user. This will add the schema to the `tap_schema.schemas` table with the 
specified owner and enable the owner to further manage that schema. If not configured, creating a
schema through the REST API is not permitted.

The optional _createSchemaInDB_ flag is set to true, a schema created by admin will be created in 
the database in addition to being added to the `tap_schema`. If false, `youcat` will not create 
the schema in the database and just assume it exists and that the `tapadm` pool has permission 
to create objects (tables and indices) in it.

As hard-coded behaviours of `youcat` are extracted from the build and made configurable,
the configuration options will usually be in this file (see **development plans** below).

The `youcat` implementation is currently hard-coded to use a PostgreSQL backend database.

## development plans

Make all aspects of the service configurable at runtime so deployers can use
the standard image published by CADC:

- document database requirements and configuration
- document user allocation process (admin API)
- move some code from youcat into appropriate opencadc library
- extract PluginFactory.properties from inside war to enable more configuration at runtime (cadc-tap-server)
- document integration test requirements (currently very CADC-specific and not usable by external developers)

## enhancements

Add *tap_schema* content management and configuration for non-user tables (partial prototype)

Add support for the *tap_schema* to be in a different database from the content (cadc-tap-server)

Add configuration option to disable user-tables so deployed service is a normal TAP service? depends on 
WD-TAP-1.2 progress







