# Sample TAP with MySQL

## MySQL 8

This library and example are based off (and tested with) [MySQL 8](http://www.mysql.com).  All Docker deployments use 
the official `mysql:8` Docker image.


## Using this Example Application

### Build the Web Application

1.  `gradle clean build`  
This will produce a `tap-example##1000.war` file in `build/libs/`.  Copy it into the 
`docker/` folder.

1.  Deploy the TAP example with [docker-compose](https://github.com/docker/compose/releases) > 1.21.0:
    1.  `cd docker/`
    1.  `docker-compose up`
