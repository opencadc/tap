# youcat

YouCat is a complete TAP service implementation that supports TAP-1.1 plus
CADC/CANFAR extensions to enable users to create their own tables and load
data into them.

## TODO

Make all aspects of the service configurable at runtime so deployers can use
the standard image published by CADC:

- move some code from youcat into appropriate opencadc library

- extract PluginFactory.properties from inside war

- document database requirements and configuration

- include support for standard token authentication (no local code needed)

## enhancements

Add *tap_schema* content management and configuration for non-user tables.

Add configuration to disable user-tables so deployed service is a normal TAP service.







