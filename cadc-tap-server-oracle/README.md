# cadc-tap-server-oracle 1.0.0

## Oracle 11g

The [build.gradle](build.gradle) file contains the necessary elements to download the Oracle JDBC client libraries
for the [Oracle 11g](http://www.oracle.com/technetwork/database/database-technologies/express-edition/overview/index.html) database.

### Building

You will first need to [Create an account with Oracle](http://www.oracle.com/webapps/maven/register/license.html) to be
permitted access to the JDBC client libraries.  Once registered, your `username` is the E-mail address supplied to 
Oracle.

The [Gradle build file](build.gradle) expects credentials on building:

```groovy
...
            username = mavenOracleUsername
            password = mavenOraclePassword
...
```

The `mavenOracleUsername` and `mavenOraclePassword` are necessary variables to download the Oracle JDBC Client 
libraries.  You can replace the variables in the [build.gradle](build.gradle) directly, or a better solution is to 
create a private `~/.gradle/gradle.properties` file.

#### `~/.gradle/gradle.properties`
```groovy
mavenOracleUsername=myemail@myprovider.com
mavenOraclePassword=mypassword
``` 

Which Gradle will automatically consume.  Those 