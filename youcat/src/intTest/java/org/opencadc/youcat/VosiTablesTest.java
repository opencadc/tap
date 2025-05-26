
package org.opencadc.youcat;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.net.ContentType;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.tap.schema.SchemaDesc;
import ca.nrc.cadc.tap.schema.TableDesc;
import ca.nrc.cadc.tap.schema.TapSchema;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vosi.TableReader;
import ca.nrc.cadc.vosi.TableSetReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class VosiTablesTest
{
    private static final Logger log = Logger.getLogger(VosiTablesTest.class);

    static
    {
        Log4jInit.setLevel("org.opencadc.youcat", Level.INFO);
    }
    
    URL tablesURL;
    
    public VosiTablesTest()
    {
        RegistryClient rc = new RegistryClient();
        this.tablesURL = rc.getServiceURL(Constants.RESOURCE_ID, Standards.VOSI_TABLES_11, AuthMethod.ANON);
    }

    @Test
    public void testValidateTablesetDoc()
    {
        try
        {
            TableSetReader tsr = new TableSetReader(true);
            log.info("testValidateTablesetDoc: " + tablesURL.toExternalForm()); 
            
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(tablesURL, bos);
            get.run();
            Assert.assertEquals(200, get.getResponseCode());
            ContentType ct = new ContentType(get.getContentType());
            Assert.assertEquals("text/xml", ct.getBaseType());
            
            TapSchema ts = tsr.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(ts);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testValidateTableDoc()
    {
        try
        {
            TableReader tr = new TableReader(true);
            String s = tablesURL.toExternalForm() + "/tap_schema.tables";
            log.info("testValidateTableDoc: " + s);
            
            URL url = new URL(s);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertEquals(200, get.getResponseCode());
            ContentType ct = new ContentType(get.getContentType());
            Assert.assertEquals("text/xml", ct.getBaseType());
            
            TableDesc td = tr.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(td);
            Assert.assertEquals("tap_schema.tables", td.getTableName());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testTableNotFound()
    {
        try
        {
            String s = tablesURL.toExternalForm() + "/tap_schema.no_such_table";
            log.info("testTableNotFound: " + s);

            URL url = new URL(s);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertEquals(404, get.getResponseCode());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testDetailMin()
    {
        try
        {
            TableSetReader tsr = new TableSetReader(true);
            String s = tablesURL.toExternalForm() + "?detail=min";
            log.info("testDetailMin: " + s);

            URL url = new URL(s);
            TapSchema ts = tsr.read(url.openStream());
            Assert.assertNotNull(ts);
            Assert.assertFalse(ts.getSchemaDescs().isEmpty());
            SchemaDesc sd = ts.getSchema("tap_schema");
            log.debug("testDetailMin: " + sd.getSchemaName());
            Assert.assertFalse(sd.getTableDescs().isEmpty());
            for (TableDesc td : sd.getTableDescs())
            {
                Assert.assertTrue("no columns:" + td.getTableName(), td.getColumnDescs().isEmpty());
            }
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testValidateVOTableDoc() {
        try {
            String s = tablesURL.toExternalForm() + "/tap_schema.tables";
            log.info("testValidateVOTableDoc: " + s);

            URL url = new URL(s);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(url, bos);
            get.setRequestProperty("Accept", VOTableWriter.CONTENT_TYPE);
            get.run();
            Assert.assertNull(get.getThrowable());
            Assert.assertEquals(200, get.getResponseCode());
            Assert.assertEquals(VOTableWriter.CONTENT_TYPE, get.getContentType());

            log.debug("VOTable XML: " + bos.toString(StandardCharsets.UTF_8));

            VOTableReader tr = new VOTableReader(true);
            VOTableDocument td = tr.read(new ByteArrayInputStream(bos.toByteArray()));
            Assert.assertNotNull(td);
            Assert.assertFalse("tap_schema.tables", td.getResources().isEmpty());
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
