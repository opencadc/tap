# youcat

YouCat is a complete TAP service implementation that supports TAP-1.1 plus
CADC/CANFAR extensions to enable users to create their own tables and load
data into them.

## configuration

The following configuration files must be available in the `/config` directory.

### catalina.properties

This file contains java system properties to configure the tomcat server and some
of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties. 

`baldur` includes multiple IdentityManager implementations to support authenticated access:
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
  
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

## youcat.properties

As hard-coded behaviours of `youcat` are extracted from the build and made configurable,
the configuration options will usually be in this file (see **development plans** below).

The `youcat` implementation is currently hard-coded to use a PostgreSQL backend database.

## development plans

Make all aspects of the service configurable at runtime so deployers can use
the standard image published by CADC:

- move some code from youcat into appropriate opencadc library

- extract PluginFactory.properties from inside war (cadc-tap-server)

- document database requirements and configuration

- include support for standard token authentication (no local code needed)

## enhancements

Add *tap_schema* content management and configuration for non-user tables.

Add configuration to disable user-tables so deployed service is a normal TAP service.







