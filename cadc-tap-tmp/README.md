# cadc-tap-tmp (1.0.0)

Simple library to provide default storage on disk for uploaded files to a TAP service.  Useful for a zero-config
TAP service to provide limited upload storage in a temporary file system.

## Applying cadc-tap-tmp

### Configuration
A configuration file `cadc-tap-tmp.properties` needs to be accessible in the `System.getProperty("user.home")/config` folder, or in a separate folder specified by a System property called `ca.nrc.cadc.util.PropertiesReader.dir`.
```properties
org.opencadc.tap.baseStorageDir = /tmp
org.opencadc.tap.baseURL = https://example.com/storage/endpoint
```

The `/files` endpoint will be appended to the `org.opencadc.tap.baseURL` property, and will be enabled below (see [Enable Retrieval](#enable-retrieval)).

### Enable storage
To enable temporary uploads to disk, set the `InlineContentHandler` in the handling UWS (`cadc-rest`) servlet to be the `TempStorageManager`:

In the `web.xml`:

```xml
<servlet>
  ...
  <init-param>
    <param-name>ca.nrc.cadc.rest.InlineContentHandler</param-name>
    <param-value>org.opencadc.tap.tmp.TempStorageManager</param-value>
  </init-param>
  ...
</servlet>

```

Finally, the implementation service's `PluginFactory.properties` will need to use the `TempStorageManger` as the `ResultStore`:

In the `PluginFactory.properties`:
```properties
ca.nrc.cadc.tap.ResultStore = org.opencadc.tap.tmp.TempStorageManager
```

### Enable retrieval
To enable retrieving the uploaded file, such as an asynchronous query, a new `/files` endpoint will be required.  Provide a concrete
implementation of the `TempStorageGetAction`:

In the `web.xml`:

```xml
    <servlet>
        <servlet-name>TempStorageServlet</servlet-name>
        <servlet-class>ca.nrc.cadc.rest.RestServlet</servlet-class>
    
        <!-- Optional init parameter to validate configuration. -->
        <init-param>
            <param-name>init</param-name>
            <param-value>ca.nrc.cadc.sc2tap.TempStorageInitAction</param-value>
        </init-param>

        <init-param>
            <param-name>get</param-name>
            <param-value>org.opencadc.tap.tmp.TempStorageGetAction</param-value>
        </init-param>
        <load-on-startup>3</load-on-startup>
    </servlet>
    
    <servlet-mapping>
        <servlet-name>TempStorageServlet</servlet-name>
        <url-pattern>/files/*</url-pattern>
    </servlet-mapping>
```
