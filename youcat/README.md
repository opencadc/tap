# table query service with user-managed tables (youcat)

This service is a complete TAP service implementation that supports the
IVOA <a href="http://www.ivoa.net/documents/TAP/20190927/">TAP-1.1</a> web 
service API plus CADC/CANFAR extensions to enable users to create their own 
tables and load data into them. 

The extended features and current implementation are aimed primarily at 
creating and bulk-loading of astronomical catalogues, but it can be used 
as a generic TAP implementation(see configuration limitations, development
plans, and enahncements needed below).

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

### cadc-registry.properties

The `youcat` service currently queries a known (trusted) GMS service for all groups a user belongs to
in order to mangle queries on the *tap_schema* content. A service providing the 
`ivo://ivoa.net/std/GMS#search-1.0` capability must be configured here for that to work.

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### cadc-tap-tmp.properties

This service uses the `cadc-tap-tmp` library to persist tap_upload tables and async results. It is currently
hard-coded (PluginFactory.properties) to use the HttpStorageManager and persist to an external URL (HTTP PUT).

See <a href="https://github.com/opencadc/tap/tree/master/cadc-tap-tmp">cadc-tap-tmp</a>.

## youcat.properties

As hard-coded behaviours of `youcat` are extracted from the build and made configurable,
the configuration options will usually be in this file (see **development plans** below).

The `youcat` implementation is currently hard-coded to use a PostgreSQL backend database.

## development plans

Make all aspects of the service configurable at runtime so deployers can use
the standard image published by CADC:

- document database requirements and configuration
- document user allocation process (schema creation and addition to *tap_schema*), maybe provide tools
- move some code from youcat into appropriate opencadc library
- extract PluginFactory.properties from inside war to enable more configuration at runtime (cadc-tap-server)
- document integration test requirements (currently very CADC-specific and not usable by external developers)

## enhancements

Add *tap_schema* content management and configuration for non-user tables.

Add support for the *tap_schema* to be in a different database from the content.

Add configuration option to disable user-tables so deployed service is a normal TAP service? This might just be
a separate build/service/image... TBD.







