# cadc-tap-tmp

Simple library to provide plugins that implement both the ResultStore (TAP-async 
result storage) and UWSInlineContentHandler (inline TAP_UPLOAD support) interfaces. 
Two interfaces are provided: `org.opencadc.tap.tmp.TempStorageManager` uses a configurable
local directory in the filesystem, `org.opencadc.tap.tmp.HttpStorageManager` uses an 
external HTTP service to store and deliver files.

## Applying cadc-tap-tmp

### Configuration
A configuration file `cadc-tap-tmp.properties` needs to be accessible in the `System.getProperty("user.home")/config` folder, or in a separate folder specified by a System property called `ca.nrc.cadc.util.PropertiesReader.dir`.

For the local filesystem using `TempStorageManager`:
```properties
org.opencadc.tap.tmp.TempStorageManager.baseURL = {base URL for result files}

org.opencadc.tap.tmp.TempStorageManager.baseStorageDir = {local directory for tmp files}
```
For the TempStorageManager, an additional servlet must be deployed in the TAP 
service to [Enable Retrieval](#enable-retrieval)

For the external http service using `HttpStorageManager`:
```properties
org.opencadc.tap.tmp.HttpStorageManager.baseURL = {base URL for result files}

org.opencadc.tap.tmp.HttpStorageManager.certificate = {certificate file name}
```
For the HttpStorageManager, the result will be PUT to that same URL and requires 
an X509 cliejnt certificate to authenticate. The certificate is located in 
{user.home}/.ssl/{certificate file name}.

In both cases, result files will be retrievable from {baseURL}/{result_filename}.


### Enable storage
To enable temporary uploads to disk, configure the `InlineContentHandler` in both the 
TAP-sync and TAP-async servlets to load the one of the plugin classes:

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

The ResultStore implementation is configured in the TAP service's
`PluginFactory.properties`, e.g.:

```properties
ca.nrc.cadc.tap.ResultStore = org.opencadc.tap.tmp.TempStorageManager
```

### Enable retrieval for TempStorageManager
To enable retrieval of the stored file, such as an asynchronous query result, 
a new endpoint will be required using the `TempStorageInitAction` and
`TempStorageGetAction`:

```xml
    <servlet>
        <servlet-name>TempStorageServlet</servlet-name>
        <servlet-class>ca.nrc.cadc.rest.RestServlet</servlet-class>
    
        <!-- Optional init parameter to validate configuration. -->
        <init-param>
            <param-name>init</param-name>
            <param-value>org.opencadc.tap.tmp.TempStorageInitAction</param-value>
        </init-param>

        <init-param>
            <param-name>get</param-name>
            <param-value>org.opencadc.tap.tmp.TempStorageGetAction</param-value>
        </init-param>
        <load-on-startup>3</load-on-startup>
    </servlet>
    
    <servlet-mapping>
        <servlet-name>TempStorageServlet</servlet-name>
        <url-pattern>/stuff-to-keep-and-serve/*</url-pattern>
    </servlet-mapping>
```
The `baseURL` in `cadc-tap-tmp.properties` must include the path component used in the above
servlet-mapping, e.g. `https://example.net/tap/stuff-to-keep-and-serve`.
