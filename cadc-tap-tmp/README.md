# cadc-tap-tmp (1.0.0)

Simple library to provide default storage on disk for uploaded files to a TAP service.  Useful for a zero-config
TAP service to provide limited upload storage in a temporary file system.

## Applying cadc-tap-tmp

### Enable storage
To enable temporary uploads to disk, set the `InlineContentHandler` in the handling UWS (`cadc-rest`) servlet to be an implementation of the `TempStorageManager`:

```java
public class MyTempStorageManager extends TempStorageManager {
    @Override
    public String getBaseURL() {
        return "https://mysite.com/myservice";
    }
    
    @Override
    public String getBaseDir() {
        return "/tmp";
    }
}
```

In the `web.xml`:

```xml
<servlet>
  ...
  <init-param>
    <param-name>ca.nrc.cadc.rest.InlineContentHandler</param-name>
    <param-value>org.example.MyTempStorageManager</param-value>
  </init-param>
  ...
</servlet>

```

This will use the temporary storage.

### Enable retrieval
To enable retrieving the uploaded file, such as an asynchronous query, a new `/files` endpoint will be required.  Provide a concrete
implementation of the `TempStorageGetAction`:

```java
public class MyTempStorageGetAction extends TempStorageGetAction {
    @Override
    public TempStorageManager getTempStorageManager() {
        return new MyTempStorageManager();
    }
}
```

In the `web.xml`:

```xml
    <servlet>
        <servlet-name>TempStorageServlet</servlet-name>
        <servlet-class>ca.nrc.cadc.rest.RestServlet</servlet-class>
        <init-param>
            <param-name>get</param-name>
            <param-value>org.example.MyTempStorageGetAction</param-value>
        </init-param>
        <load-on-startup>3</load-on-startup>
    </servlet>
    
    <servlet-mapping>
        <servlet-name>TempStorageServlet</servlet-name>
        <url-pattern>/files/*</url-pattern>
    </servlet-mapping>
```
