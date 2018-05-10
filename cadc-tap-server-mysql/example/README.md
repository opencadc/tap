# Sample TAP with Oracle

## Oracle 11g

The [build.gradle](build.gradle) file contains the necessary elements to download the Oracle JDBC client libraries
for the [Oracle 11g](http://www.oracle.com/technetwork/database/database-technologies/express-edition/overview/index.html) database.

### Building

You will first need to [Create an account with Oracle](http://www.oracle.com/webapps/maven/register/license.html) to be
permitted access to the JDBC client libraries.  Once registered, your `username` is the E-mail address supplied to 
Oracle.

The [Gradle build file](build.gradle) expects credentials on building (in the `repositories` section):

```groovy
            username = mavenOracleUsername
            password = mavenOraclePassword
```

The `mavenOracleUsername` and `mavenOraclePassword` are necessary variables to download the Oracle JDBC Client 
libraries.  You can replace the variables in the [build.gradle](build.gradle) directly, or a better solution is to 
create a private `~/.gradle/gradle.properties` file.

#### `~/.gradle/gradle.properties`
```groovy
mavenOracleUsername=myemail@myprovider.com
mavenOraclePassword=mypassword
``` 

Which Gradle will automatically consume.


### Implementations

Any implementations (i.e. Full TAP services that _use_ this library) will need to also declare the Maven repository at 
the beginning of their `build.gradle` because the WAR file will incorporate the JAR files:

```groovy
repositories {
    mavenLocal()
    jcenter()

    maven {
        name 'maven.oracle.com'
        url 'https://maven.oracle.com'
        credentials {
            username = mavenOracleUsername
            password = mavenOraclePassword
        }
    }
}
```

## Using this Example Application

### Build the Web Application

1.  `gradle clean build`  
This will produce a `tap-example##1000.war` file in `build/libs/`.  Copy it into the 
`docker/` folder.

1.  Deploy the TAP example with [docker-compose](https://github.com/docker/compose/releases) > 1.21.0:
    1.  `cd docker/`
    1.  `docker-compose up`
