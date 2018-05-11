
# Example TAP service

This is a minimal sample TAP service implementation.

## Files

* AdqlQueryImpl.java - a sample ADQL query processor. The sample here includes the default
actions from the base class (AdqlQuery.java from cadcTAP): 

syntax validation
validate tables and columns against tap_schema
validate use of BLOB and CLOB columns in select-list only
convert * in select-list to a fixed list if columns from the tap_schema
extract select-list and match it to tap_schema.columns to supprot correct output metadata

The sample also includes using the TopConverter to convert "select top N ..." to the 
"select ... limit N" construct used by some DBs (e.g. PostgreSQL). This can be removed if your DB
supports TOP directly. The AdqlQueryImpl class is the place to put your own custom query processing. 

* [SampleJobManager.java](src/main/java/ca/nrc/cadc/sample/SampleJobManager.java) - a sample implementation of the JobManager interface in 
cadc-uws. This configures the management and persistence of UWS jobs.

* [ResultStoreImpl.java](src/main/java/ca/nrc/cadc/sample/ResultStoreImpl.java) - a sample class that saves async query results in the 
local filesystem and returns a URL so they can be retrieved. See: #3 in TODO below.

* [context.xml](src/main/webapp/META-INF/context.xml) - maps a globally defined DataSource to the expected local name used by 
the [QueryRunner](https://github.com/opencadc/tap/blob/master/cadc-tap-server/src/main/java/ca/nrc/cadc/tap/QueryRunner.java).

* [web.xml](src/main/webapp/WEB-INF/web.xml) - example deployment descriptor, includes sync and async resources and the 
[LogControlServlet](https://github.com/opencadc/core/blob/master/cadc-log/src/main/java/ca/nrc/cadc/log/LogControlServlet.java) from [cadc-log](https://github.com/opencadc/core/tree/master/cadc-log). 
It also includes servlets for the VOSI-availability and VOSI-tables resources.


## TODO

1. Make sure you can build the project with [Gradle](http://www.gradle.org) (2.5 and 3 are known to work)

1. Tweak the context.xml to map the global data source name from the app server config.

1. Set the values in the [ResultStoreImpl.properties](src/main/resources/ResultStoreImpl.properties) config file to suit your system setup.

1. This example uses [PostgreSQL](http://www.postgresql.org) as the backend for the TAP Schema.  See the [Example 
build.gradle](build.gradle) file to swap out a different database, then rebuild the Web Application (WAR).

1. Create the tap_schema tables in your DB server and insert at least the content 
that describes the TAP_SCHEMA tables themselves (SQL from cadc-tap-schema) . Make sure the datasource 
for tap queries (#2) can read it. Recommendation: create a special account with read-only permission
to the TAP_SCHEMA and any tables you intend to expose via the TAP service and use this account with
the "jdbc/tapuser" data source. 

1. Build and deploy the war file. We expect that the app has to be reachable 
via the default port 80 (some generated redirects are known to not include the port number - a bug - so I 
recommend either an AJP or HTTP proxy rule from apache to the app server as the normal config).

1. Try the test request in the tsQuery script (assumes localhost as it is for developers).


Contact [pdowler.cadc@gmail.com](mailto:pdowler.cadc@gmail.com) if you need help.

