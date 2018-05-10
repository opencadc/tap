# Sample TAP with Sybase ASE

## Sybase ASE 15.x

The [build.gradle](build.gradle) file contains the necessary elements to download the Oracle JDBC client libraries
for the [Sybase ASE](https://www.sap.com/canada/products/sybase-ase.html) database.


## Using this Example Application

The Sybase ASE database does not lend itself well to being containerized.  Therefore, this
example application relies on an existing database.  The Sybase support is really intended
for use by the [Canadian Astronomy Data Centre](http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/),
but any who wishes to use it is welcome.


### Build the Web Application

1.  `gradle clean build`  
This will produce a `tap-example##1000.war` file in `build/libs/`.  Copy it into the 
`docker/` folder.

1.  Deploy the TAP example with [docker-compose](https://github.com/docker/compose/releases) > 1.21.0:
    1.  `cd docker/`
    1.  `docker-compose up`
