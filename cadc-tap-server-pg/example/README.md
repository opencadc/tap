# Sample TAP with PostgreSQL

## PostgreSQL 10

The [build.gradle](build.gradle) file contains the necessary elements to download the PostgreSQL JDBC client libraries
for the [PostgreSQL 10](http://www.postgresql.org) database.


## Using this Example Application

### Build the Web Application

1.  `gradle clean build`  
This will produce a `tap-example##1000.war` file in `build/libs/`.  Copy it into the 
`docker/` folder.

1.  Deploy the TAP example with [docker-compose](https://github.com/docker/compose/releases) > 1.21.0:
    1.  `cd docker/`
    1.  `docker-compose up`